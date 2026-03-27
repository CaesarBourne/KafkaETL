export function transform(row: any) {
  const amount = Number(row.amount)

  if (!row.userId || isNaN(amount)) {
    throw new Error("Invalid data")
  }

  return {
    userId: row.userId,
    amount
  }
}