import { pool } from "../database/pool.js";

export function createCustomerRepository(database = pool) {
    async function findByUid(uid) {
        const result = await database.query(
            `
        SELECT c.uid, c.fullname, c.mid, m.mname, m.score
        FROM customers AS c
        JOIN memberships AS m ON m.mid = c.mid
        WHERE c.uid = $1
      `,
            [uid]
        );

        return result.rows[0] ?? null;
    }

    async function findMembershipById(mid) {
        const result = await database.query(
            `
        SELECT mid, mname, score
        FROM memberships
        WHERE mid = $1
      `,
            [mid]
        );

        return result.rows[0] ?? null;
    }

    async function createDefault({ uid, fullname }) {
        await database.query(
            `
        INSERT INTO customers (uid, fullname, mid)
        VALUES ($1, $2, 1)
        ON CONFLICT (uid) DO NOTHING
      `,
            [uid, fullname]
        );

        return findByUid(uid);
    }

    async function update(uid, { fullname, mid }) {
        const result = await database.query(
            `
        WITH updated AS (
          UPDATE customers
          SET fullname = $2,
              mid = COALESCE($3, mid)
          WHERE uid = $1
          RETURNING uid, fullname, mid
        )
        SELECT c.uid, c.fullname, c.mid, m.mname, m.score
        FROM updated AS c
        JOIN memberships AS m ON m.mid = c.mid
      `,
            [uid, fullname, mid ?? null]
        );

        return result.rows[0] ?? null;
    }

    return {
        findByUid,
        findMembershipById,
        createDefault,
        update,
    };
}

export const customerRepository = createCustomerRepository();