CREATE TABLE stages (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    capacity INT NOT NULL
);

CREATE TABLE webhooks (
    id BIGSERIAL PRIMARY KEY,
    url VARCHAR(512) NOT NULL,
    secret VARCHAR(255) NOT NULL,
    client_type VARCHAR(50) NOT NULL
);

CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,
    stage_id BIGINT,
    type VARCHAR(50) NOT NULL,
    message VARCHAR(512) NOT NULL,
    severity VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE notification_attempts (
    id BIGSERIAL PRIMARY KEY,
    alert_id BIGINT,
    webhook_url VARCHAR(512) NOT NULL,
    attempt_number INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO stages (id, name, capacity) VALUES (1, 'Main Stage', 1000);
INSERT INTO stages (id, name, capacity) VALUES (2, 'Dance Arena', 500);