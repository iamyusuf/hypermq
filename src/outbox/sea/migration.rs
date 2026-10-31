//! Schema for the outbox tables.
//!
//! Column types differ on MySQL: `DATETIME` instead of `TIMESTAMP` (which overflows in 2038),
//! and `LONGBLOB` for bodies, since `BLOB` stops at 64 KiB.

use sea_orm::DbBackend;
use sea_orm_migration::prelude::*;

// Short names for the column enums below. The enum names set the table names.
use self::OutboxDead as Dead;
use self::OutboxMessages as Messages;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(CreateOutbox)]
    }
}

/// The table name comes from the enum name in snake case: `outbox_messages`.
#[derive(DeriveIden)]
enum OutboxMessages {
    Table,
    Seq,
    MessageId,
    TargetKind,
    Target,
    RoutingKey,
    ContentType,
    Headers,
    Body,
    Attempts,
    CreatedAt,
    NextAttemptAt,
    LastError,
}

/// The table name comes from the enum name in snake case: `outbox_dead`.
#[derive(DeriveIden)]
enum OutboxDead {
    Table,
    Seq,
    MessageId,
    TargetKind,
    Target,
    RoutingKey,
    ContentType,
    Headers,
    Body,
    Attempts,
    CreatedAt,
    DeadAt,
    Reason,
}

struct CreateOutbox;

impl MigrationName for CreateOutbox {
    fn name(&self) -> &str {
        "m20260101_000001_create_outbox"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for CreateOutbox {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let mysql = manager.get_database_backend() == DbBackend::MySql;

        manager
            .create_table(
                Table::create()
                    .table(Messages::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Messages::Seq)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Messages::MessageId)
                            .string_len(64)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Messages::TargetKind)
                            .string_len(16)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Messages::Target).string_len(255).not_null())
                    .col(
                        ColumnDef::new(Messages::RoutingKey)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Messages::ContentType)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Messages::Headers).text().not_null())
                    .col(body_column(Messages::Body, mysql))
                    .col(
                        ColumnDef::new(Messages::Attempts)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(timestamp_column(Messages::CreatedAt, mysql))
                    .col(timestamp_column(Messages::NextAttemptAt, mysql))
                    .col(ColumnDef::new(Messages::LastError).string_len(255))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_outbox_messages_order")
                    .table(Messages::Table)
                    .col(Messages::TargetKind)
                    .col(Messages::Target)
                    .col(Messages::RoutingKey)
                    .col(Messages::Seq)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_outbox_messages_created")
                    .table(Messages::Table)
                    .col(Messages::CreatedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Dead::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Dead::Seq)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Dead::MessageId).string_len(64).not_null())
                    .col(ColumnDef::new(Dead::TargetKind).string_len(16).not_null())
                    .col(ColumnDef::new(Dead::Target).string_len(255).not_null())
                    .col(ColumnDef::new(Dead::RoutingKey).string_len(255).not_null())
                    .col(ColumnDef::new(Dead::ContentType).string_len(255).not_null())
                    .col(ColumnDef::new(Dead::Headers).text().not_null())
                    .col(body_column(Dead::Body, mysql))
                    .col(ColumnDef::new(Dead::Attempts).integer().not_null())
                    .col(timestamp_column(Dead::CreatedAt, mysql))
                    .col(timestamp_column(Dead::DeadAt, mysql))
                    .col(ColumnDef::new(Dead::Reason).string_len(64).not_null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Dead::Table).if_exists().to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(Messages::Table).if_exists().to_owned())
            .await
    }
}

fn body_column<T: IntoIden>(name: T, mysql: bool) -> ColumnDef {
    let mut column = ColumnDef::new(name);
    if mysql {
        column.custom(Alias::new("LONGBLOB"));
    } else {
        column.blob();
    }
    column.not_null().take()
}

fn timestamp_column<T: IntoIden>(name: T, mysql: bool) -> ColumnDef {
    let mut column = ColumnDef::new(name);
    if mysql {
        column.date_time();
    } else {
        column.timestamp_with_time_zone();
    }
    column.not_null().take()
}
