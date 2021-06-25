create schema if not exists pm;

create type pm.payout_status as enum ('UNPAID', 'PAID', 'CONFIRMED', 'CANCELLED');

create table if not exists pm.payout
(
    id             bigserial                   not null,
    sequence_id    integer                     not null,
    payout_id      varchar                     not null,
    created_at     timestamp without time zone not null,
    party_id       varchar                     not null,
    shop_id        varchar                     not null,
    status         pm.payout_status            not null,
    payout_tool_id varchar,
    amount         bigint                      not null,
    fee            bigint                      not null default 0,
    currency_code  varchar                     not null,
    cancel_details varchar,
    constraint payout_id_pkey primary key (id),
    constraint payout_payout_id_ukey unique (payout_id)
);

create type pm.account_type as enum ('MERCHANT_SETTLEMENT', 'MERCHANT_GUARANTEE', 'MERCHANT_PAYOUT',
    'PROVIDER_SETTLEMENT', 'SYSTEM_SETTLEMENT', 'EXTERNAL_INCOME', 'EXTERNAL_OUTCOME');

create table if not exists pm.cash_flow_posting
(
    id                bigserial                   not null,
    payout_id         varchar                     not null,
    created_at        timestamp without time zone not null,
    from_account_id   bigint                      not null,
    from_account_type pm.account_type             not null,
    to_account_id     bigint                      not null,
    to_account_type   pm.account_type             not null,
    amount            bigint                      not null,
    currency_code     varchar                     not null,
    description       varchar,
    constraint posting_id_pkey primary key (id)
);
