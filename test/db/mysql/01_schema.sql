-- Schéma MySQL pour les tests demodata
CREATE TABLE IF NOT EXISTS users (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    first_name  VARCHAR(64)  NOT NULL,
    last_name   VARCHAR(64)  NOT NULL,
    email       VARCHAR(128) NOT NULL,
    phone       VARCHAR(20)  NOT NULL,
    birthdate   DATE         NOT NULL,
    address     VARCHAR(255) NOT NULL,
    postal_code CHAR(5)      NOT NULL,
    city        VARCHAR(64)  NOT NULL,
    country     VARCHAR(64)  NOT NULL DEFAULT 'France'
);

CREATE TABLE IF NOT EXISTS orders (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    user_id     INT           NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status      VARCHAR(16)   NOT NULL DEFAULT 'pending',
    FOREIGN KEY (user_id) REFERENCES users(id)
);
