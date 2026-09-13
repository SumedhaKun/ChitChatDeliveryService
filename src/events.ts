import { z } from "zod";

export const messageCreatedSchema = z
  .object({
    messageId: z.uuid(),
    senderId: z.uuid(),
    conversationId: z.uuid(),
    content: z.string(),
    createdAt: z.iso.datetime({ offset: true }),
  })
  .strict();

export type MessageCreatedEvent = z.infer<typeof messageCreatedSchema>;

export function parseMessageCreatedEvent(
  value: Buffer | string | null,
): MessageCreatedEvent {
  if (value === null) throw new Error("Kafka message value is null");
  let decoded: unknown;
  try {
    decoded = JSON.parse(value.toString());
  } catch {
    throw new Error("Kafka message value is not valid JSON");
  }
  return messageCreatedSchema.parse(decoded);
}
