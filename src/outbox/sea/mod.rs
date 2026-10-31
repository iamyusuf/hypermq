//! SeaORM store for PostgreSQL and MySQL. ORM types stay inside this module.

mod entity;
mod migration;

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sea_orm::sea_query::{Alias, Expr, ExprTrait, LockBehavior, LockType, Query};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::{NotSet, Set},
    ColumnTrait, Database, DatabaseConnection, DatabaseTransaction, DbErr, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Select, TransactionTrait,
};
use sea_orm_migration::MigratorTrait;

use super::{
    OutboxStore, Snapshot, StoreError, after, age_seconds, expiry_cutoff, ordering_routing_key,
};
use crate::broker::{PublishRequest, PublishTarget, Publisher};
use crate::outbox::relay::{RelayPolicy, Settled, decide};
use entity::{MESSAGES_TABLE, dead, messages};

/// Rows expired per cycle, so one cycle stays bounded.
const EXPIRE_BATCH: u64 = 100;

pub struct SeaStore {
    db: DatabaseConnection,
}

impl SeaStore {
    /// Connects without logging the URL: the error text is fixed so credentials never leak.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let db = Database::connect(url).await.map_err(|_| {
            StoreError::Database("cannot connect to the outbox database".to_owned())
        })?;
        Ok(Self { db })
    }

    pub async fn migrate(url: &str) -> Result<(), StoreError> {
        let store = Self::connect(url).await?;
        migration::Migrator::up(&store.db, None)
            .await
            .map_err(db_err)
    }
}

fn db_err(err: DbErr) -> StoreError {
    StoreError::Database(err.to_string())
}

fn target_parts(target: &PublishTarget) -> (&'static str, &str) {
    match target {
        PublishTarget::Exchange(name) => ("exchange", name),
        PublishTarget::Queue(name) => ("queue", name),
    }
}

fn to_request(row: &messages::Model) -> Result<PublishRequest, String> {
    let target = match row.target_kind.as_str() {
        "exchange" => PublishTarget::Exchange(row.target.clone()),
        "queue" => PublishTarget::Queue(row.target.clone()),
        other => return Err(format!("unknown target kind `{other}`")),
    };
    let headers: BTreeMap<String, String> =
        serde_json::from_str(&row.headers).map_err(|err| format!("headers: {err}"))?;
    Ok(PublishRequest {
        target,
        routing_key: row.routing_key.clone(),
        message_id: row.message_id.clone(),
        content_type: row.content_type.clone(),
        headers,
        body: Bytes::from(row.body.clone()),
    })
}

/// Rows that are first for their ordering key and ready to try now.
///
/// A row is blocked when an earlier row (lower `seq`) has the same target and routing key, so a
/// message never overtakes an older one for the same key.
fn head_of_line(now: DateTime<Utc>) -> Select<messages::Entity> {
    let earlier = Alias::new("earlier");
    let outer = Alias::new(MESSAGES_TABLE);
    let blocked = Query::select()
        .expr(Expr::val(1))
        .from_as(Alias::new(MESSAGES_TABLE), earlier.clone())
        .and_where(
            Expr::col((earlier.clone(), messages::Column::TargetKind))
                .eq(Expr::col((outer.clone(), messages::Column::TargetKind))),
        )
        .and_where(
            Expr::col((earlier.clone(), messages::Column::Target))
                .eq(Expr::col((outer.clone(), messages::Column::Target))),
        )
        .and_where(
            Expr::col((earlier.clone(), messages::Column::RoutingKey))
                .eq(Expr::col((outer.clone(), messages::Column::RoutingKey))),
        )
        .and_where(
            Expr::col((earlier.clone(), messages::Column::Seq))
                .lt(Expr::col((outer.clone(), messages::Column::Seq))),
        )
        .to_owned();

    messages::Entity::find()
        .filter(messages::Column::NextAttemptAt.lte(now))
        .filter(Expr::exists(blocked).not())
        .order_by_asc(messages::Column::Seq)
}

async fn delete(txn: &DatabaseTransaction, seq: i64) -> Result<(), StoreError> {
    messages::Entity::delete_by_id(seq)
        .exec(txn)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn move_to_dead(
    txn: &DatabaseTransaction,
    row: &messages::Model,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    let copy = dead::ActiveModel {
        seq: Set(row.seq),
        message_id: Set(row.message_id.clone()),
        target_kind: Set(row.target_kind.clone()),
        target: Set(row.target.clone()),
        routing_key: Set(row.routing_key.clone()),
        content_type: Set(row.content_type.clone()),
        headers: Set(row.headers.clone()),
        body: Set(row.body.clone()),
        attempts: Set(row.attempts),
        created_at: Set(row.created_at),
        dead_at: Set(now),
        reason: Set(reason.to_owned()),
    };
    dead::Entity::insert(copy)
        .exec_without_returning(txn)
        .await
        .map_err(db_err)?;
    delete(txn, row.seq).await
}

/// Writes the outcome of one relay attempt inside the claiming transaction.
async fn apply(
    txn: &DatabaseTransaction,
    row: &messages::Model,
    settled: Settled,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    match settled {
        Settled::Delivered => delete(txn, row.seq).await,
        Settled::Dead { reason } => move_to_dead(txn, row, reason, now).await,
        Settled::Retry {
            attempts,
            delay,
            last_error,
        } => {
            let mut active: messages::ActiveModel = row.clone().into();
            active.attempts = Set(i32::try_from(attempts).unwrap_or(i32::MAX));
            active.next_attempt_at = Set(after(now, delay));
            active.last_error = Set(Some(last_error.to_owned()));
            active.update(txn).await.map_err(db_err)?;
            Ok(())
        }
        Settled::BrokerDown => Ok(()),
    }
}

#[async_trait]
impl OutboxStore for SeaStore {
    async fn insert(
        &self,
        request: &PublishRequest,
        now: DateTime<Utc>,
        max_messages: u64,
    ) -> Result<bool, StoreError> {
        let txn = self.db.begin().await.map_err(db_err)?;
        let depth = messages::Entity::find().count(&txn).await.map_err(db_err)?;
        if depth >= max_messages {
            return Ok(false);
        }

        let (kind, target) = target_parts(&request.target);
        let headers = serde_json::to_string(&request.headers)
            .map_err(|err| StoreError::Database(format!("cannot encode headers: {err}")))?;
        let row = messages::ActiveModel {
            seq: NotSet,
            message_id: Set(request.message_id.clone()),
            target_kind: Set(kind.to_owned()),
            target: Set(target.to_owned()),
            routing_key: Set(ordering_routing_key(request).to_owned()),
            content_type: Set(request.content_type.clone()),
            headers: Set(headers),
            body: Set(request.body.to_vec()),
            attempts: Set(0),
            created_at: Set(now),
            next_attempt_at: Set(now),
            last_error: Set(None),
        };
        messages::Entity::insert(row)
            .exec_without_returning(&txn)
            .await
            .map_err(db_err)?;
        txn.commit().await.map_err(db_err)?;
        Ok(true)
    }

    async fn expire(&self, policy: &RelayPolicy, now: DateTime<Utc>) -> Result<u64, StoreError> {
        let cutoff = expiry_cutoff(now, policy.max_age);
        let txn = self.db.begin().await.map_err(db_err)?;
        let rows = messages::Entity::find()
            .filter(messages::Column::CreatedAt.lt(cutoff))
            .order_by_asc(messages::Column::Seq)
            .limit(EXPIRE_BATCH)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&txn)
            .await
            .map_err(db_err)?;
        for row in &rows {
            move_to_dead(&txn, row, "expired", now).await?;
        }
        txn.commit().await.map_err(db_err)?;
        Ok(u64::try_from(rows.len()).unwrap_or(u64::MAX))
    }

    async fn relay_one(
        &self,
        publisher: &dyn Publisher,
        policy: &RelayPolicy,
        now: DateTime<Utc>,
    ) -> Result<Option<Settled>, StoreError> {
        let txn = self.db.begin().await.map_err(db_err)?;
        let row = head_of_line(now)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(db_err)?;
        let Some(row) = row else {
            txn.commit().await.map_err(db_err)?;
            return Ok(None);
        };

        let settled = match to_request(&row) {
            Ok(request) => {
                let attempts = u32::try_from(row.attempts).unwrap_or(0);
                decide(&publisher.publish(request).await, attempts, policy)
            }
            Err(reason) => {
                tracing::error!(message_id = %row.message_id, %reason, "outbox row is corrupt; dead-lettering");
                Settled::Dead { reason: "corrupt" }
            }
        };
        apply(&txn, &row, settled, now).await?;
        txn.commit().await.map_err(db_err)?;
        Ok(Some(settled))
    }

    async fn snapshot(&self, now: DateTime<Utc>) -> Result<Snapshot, StoreError> {
        let depth = messages::Entity::find()
            .count(&self.db)
            .await
            .map_err(db_err)?;
        let oldest = messages::Entity::find()
            .order_by_asc(messages::Column::CreatedAt)
            .one(&self.db)
            .await
            .map_err(db_err)?;
        let dead_letters = dead::Entity::find().count(&self.db).await.map_err(db_err)?;
        Ok(Snapshot {
            depth,
            oldest_age_seconds: oldest.map_or(0, |row| age_seconds(now, row.created_at)),
            dead_letters,
        })
    }
}
