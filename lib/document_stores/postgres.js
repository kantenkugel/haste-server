/*global require,module,process*/

var { Pool } = require('pg');
var winston = require('winston');

// create table entries (id serial primary key, key varchar(255) not null, value text not null, expiration int, unique(key));

// A postgres document store
var PostgresDocumentStore = function (options) {
    this.expireJS = options.expire;
    this.connectionUrl = process.env.DATABASE_URL || options.connectionUrl;
    this.pool = new Pool({
        connectionString: this.connectionUrl
    });
    
    process.on('exit', () => {
        this.pool.end();
    });
};

PostgresDocumentStore.prototype = {

    // Set a given key
    set: function (key, data, callback, skipExpire) {
        var now = Math.floor(new Date().getTime() / 1000);

        this.pool.query('INSERT INTO entries (key, value, expiration) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = excluded.value, expiration = excluded.expiration',
            [
                key,
                data,
                this.expireJS && !skipExpire ? this.expireJS + now : null
            ],
            (err) => {
                if (err) {
                    winston.error('error persisting value to postgres', { error: err });
                    return callback(false);
                }
                callback(true);
            }
        );
    },

    // Get a given key's data
    get: function (key, callback, skipExpire) {
        var now = Math.floor(new Date().getTime() / 1000);
        this.pool.query('SELECT id,value,expiration from entries where KEY = $1 and (expiration IS NULL or expiration > $2)',
            [key, now],
            (err, result) => {
                if (err) {
                    winston.error('error retrieving value from postgres', { error: err });
                    return callback(false);
                }
                callback(result.rows.length ? result.rows[0].value : false);
                if (result.rows.length && this.expireJS && !skipExpire) {
                    this.pool.query('UPDATE entries SET expiration = $1 WHERE ID = $2', 
                        [
                            this.expireJS + now,
                            result.rows[0].id
                        ],
                        (err) => {
                            if (err)
                                winston.error('error updating expiry', { error: err });
                        }
                    );
                }
            }
        );
    }

};

module.exports = PostgresDocumentStore;
