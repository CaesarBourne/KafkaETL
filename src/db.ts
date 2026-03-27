import sqlite3 from "sqlite3"

const db = new sqlite3.Database("etl.db")

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS transactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      userId TEXT,
      amount REAL
    )
  `)
})

export function insertTransaction(userId: string, amount: number) {
  return new Promise((resolve, reject) => {
    db.run(
      "INSERT INTO transactions (userId, amount) VALUES (?, ?)",
      [userId, amount],
      function (err) {
        if (err) reject(err)
        else resolve(this.lastID)
      }
    )
  })
}