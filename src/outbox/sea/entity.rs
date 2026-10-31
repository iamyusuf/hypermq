//! SeaORM entities for the outbox tables. Column types are set by the migration.

pub const MESSAGES_TABLE: &str = "outbox_messages";

pub mod messages {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "outbox_messages")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub seq: i64,
        pub message_id: String,
        pub target_kind: String,
        pub target: String,
        pub routing_key: String,
        pub content_type: String,
        pub headers: String,
        pub body: Vec<u8>,
        pub attempts: i32,
        pub created_at: DateTimeUtc,
        pub next_attempt_at: DateTimeUtc,
        pub last_error: Option<String>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod dead {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "outbox_dead")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub seq: i64,
        pub message_id: String,
        pub target_kind: String,
        pub target: String,
        pub routing_key: String,
        pub content_type: String,
        pub headers: String,
        pub body: Vec<u8>,
        pub attempts: i32,
        pub created_at: DateTimeUtc,
        pub dead_at: DateTimeUtc,
        pub reason: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
