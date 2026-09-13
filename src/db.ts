import { eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { pgTable, primaryKey, uuid } from "drizzle-orm/pg-core";
import postgres from "postgres";

export const conversationMembers = pgTable(
  "conversation_members",
  {
    userId: uuid("user_id").notNull(),
    conversationId: uuid("conversation_id").notNull(),
  },
  (table) => [primaryKey({ columns: [table.userId, table.conversationId] })],
);

export interface MemberRepository {
  getUserIds(conversationId: string): Promise<string[]>;
}

export interface Database {
  members: MemberRepository;
  close(): Promise<void>;
}

export function createDatabase(databaseUrl: string): Database {
  const sql = postgres(databaseUrl, { prepare: false });
  const db = drizzle(sql);
  return {
    members: {
      async getUserIds(conversationId) {
        const rows = await db
          .select({ userId: conversationMembers.userId })
          .from(conversationMembers)
          .where(eq(conversationMembers.conversationId, conversationId));
        return rows.map(({ userId }) => userId);
      },
    },
    async close() {
      await sql.end();
    },
  };
}
