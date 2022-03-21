const sqlite3 = require("sqlite3").verbose();

class Storage {
  createDatabase() {
    var newdb = new sqlite3.Database("./storage.db", (err) => {
      if (err) {
        console.log("Getting error " + err);
        exit(1);
      }
      this.createTables(newdb);
    });
    return newdb;
  }

  createTables(newdb) {
    newdb.exec(
      `
        CREATE TABLE IF NOT EXISTS Ledger (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ledger_index INTEGER NOT NULL,
          close_time INTEGER NOT NULL,
          close_time_human TEXT NOT NULL
      );`,
      () => {}
    );
  }

  getInstance() {
    return new sqlite3.Database(
      "./storage.db",
      sqlite3.OPEN_READWRITE,
      (err) => {
        if (err && err.code == "SQLITE_CANTOPEN") {
          console.log(err);
          return;
        } else if (err) {
          console.log("Getting error " + err);
          exit(1);
        }
      }
    );
  }

  insert(db, ledger_index, close_time, close_time_human) {
    db.run(
      `INSERT INTO Ledger(ledger_index,close_time,close_time_human) select ?,?,? WHERE (SELECT COUNT(*) FROM Ledger WHERE ledger_index = ?) = 0`,
      [
        ledger_index,
        close_time,
        close_time_human,
        ledger_index
      ],
      function () {
        console.log("New Ledger record added with id " + this.lastID);
      }
    );
  }

  async returnClosestIndex(db, rippleEpoch, type) {
    return new Promise(function (resolve, reject) {
        let sql = '';
        if(type === 'Start')
        {
            sql = `SELECT * FROM Ledger WHERE close_time < ? ORDER BY close_time DESC LIMIT 1 `;
        } else if (type === 'End')
        {
            if(rippleEpoch === 0)
            {
                sql = `SELECT * FROM Ledger ORDER BY close_time DESC LIMIT 1 `;
            } else {
                sql = `SELECT * FROM Ledger WHERE close_time > ? ORDER BY close_time ASC LIMIT 1 `;
            }
        }
      try {
        db.all(sql, [rippleEpoch], (err, rows) => {
          if (err) {
            console.log("Error: " + err);
          }
          if (rows.length > 0) {
            resolve(rows);
          } else {
            resolve([]);
          }
        });
      } catch (err) {
        console.log(err);
      }
    });
  }
}

module.exports = Storage;
