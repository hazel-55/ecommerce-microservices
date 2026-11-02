import { pool } from "../database/pool.js";
import { DomainError } from "../../domain/errors.js";

export function createUserRepository(database = pool) {
    async function findByUsername(username) {
        const result = await database.query(
            `
        SELECT u.uid, u.username, u.password, r.rolename AS role
        FROM users AS u
        JOIN roles AS r ON r.roleid = u.roleid
        WHERE u.username = $1
      `,
            [username]
        );

        return result.rows[0] ?? null;
    }

    async function findByUid(uid) {
        const result = await database.query(
            `
        SELECT u.uid, u.username, u.email, u.phone,
               r.rolename AS role
        FROM users AS u
        JOIN roles AS r ON r.roleid = u.roleid
        WHERE u.uid = $1
      `,
            [uid]
        );

        return result.rows[0] ?? null;
    }

    async function createCustomerUser({
        username,
        passwordHash,
        email,
        phone,
    }) {
        try {
            const result = await database.query(
                `
          WITH created AS (
  INSERT INTO users (username, password, email, phone, roleid)
  VALUES ($1, $2, $3, $4, 1)
  RETURNING uid, username, email, phone, roleid
),
enqueued AS (
  INSERT INTO customer_provisioning_tasks (uid, fullname)
  SELECT uid, username
  FROM created
  RETURNING uid
)
SELECT u.uid, u.username, u.email, u.phone,
       r.rolename AS role
FROM created AS u
JOIN roles AS r ON r.roleid = u.roleid
JOIN enqueued AS task ON task.uid = u.uid
        `,
                [username, passwordHash, email, phone]
            );

            return result.rows[0];
        } catch (error) {
            if (
                error.code === "23505" &&
                ["users_username_key", "users_email_key"].includes(error.constraint)
            ) {
                throw new DomainError(
                    "ALREADY_EXISTS",
                    "Username or email already exists"
                );
            }

            throw error;
        }
    }

    return { findByUsername, findByUid, createCustomerUser };
}

export const userRepository = createUserRepository();