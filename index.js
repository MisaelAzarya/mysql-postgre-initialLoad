const pg = require('pg');
const Q = require("q");
const mysql = require('mysql');

var skip = 0;

function pgQuery(client, query, values) {
    return new Promise((resolve, reject) => {
    client.query(query, values, function(err, results) {
        if (err) {
        console.error(err);
        return reject(err);
        }
        resolve(results)
    })
    })
}
  
// function while menggunakan promise
function promiseWhile(condition, body) {
    var done = Q.defer();

    function loop() {
        if (!condition()) return done.resolve();
        Q.when(body(), loop, done.reject);
    }
    Q.nextTick(loop);

    // The promise
    return done.promise;
}

const load = async () => {
    const mysqlConnection = mysql.createConnection({
        host: "192.168.65.2",
        user: "root",
        password: "",
        port: "3306",
        database: "supersample"
    });
    mysqlConnection.connect(function(err){
        if(err)throw err;
        else{
            console.log("mysql");
        }
    })

    //syntaxnya username:password@server:port/database_name
    const pgConString = "postgres://postgres:mysecretpassword@172.17.0.2:5432/staging_ingestion";
    var clientpg = new pg.Client(pgConString);

    await clientpg.connect();
    console.log("client pg");

    var pgTable = "CREATE TABLE IF NOT EXISTS superstore (" +
                    "row_ID serial PRIMARY KEY," +
                    "order_id VARCHAR(14)," +
                    "order_date TIMESTAMP," +
                    "ship_date TIMESTAMP," +
                    "ship_mode VARCHAR(14)," +
                    "customer_id VARCHAR(8)," + 
                    "customer_name VARCHAR(22)," +
                    "segment VARCHAR(11)," +
                    "country VARCHAR(13)," +
                    "city VARCHAR(16)," +
                    "state VARCHAR(20)," +
                    "postal_code VARCHAR(6)," +
                    "region VARCHAR(7)," +
                    "product_id VARCHAR(15)," +
                    "category VARCHAR(15)," +
                    "sub_category VARCHAR(11)," +
                    "product_name VARCHAR(127)," +
                    "sales NUMERIC(9, 4)," +
                    "quantity NUMERIC(3, 1)," +
                    "discount NUMERIC(3, 2)," +
                    "profit NUMERIC(8, 4)" +
                ");";

    var pgTable2 = "CREATE TABLE IF NOT EXISTS superstore_log ("+
                    "job_id SERIAL PRIMARY KEY," +
                    "proses_tsamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP," +
                    "number_of_row INTEGER," +
                    "operation VARCHAR(10)" +
                ");";

    await clientpg.query(pgTable);
    console.log("Create Table superstore");
    await clientpg.query(pgTable2);
    console.log("Create Table superstore_log");

    clientpg.query("SELECT * FROM superstore LIMIT 1", function(err, result){
        if(err) throw err;
        else{
            var flag = 1;
            var offset = 0;
            var count = 0;
            
            // artinya kalau sudah ada data maka gk perlu masuk ke promisewhile
            if(result.rows.length > 0){
                skip = 1;
                flag = 0;
            }
            promiseWhile(function () { return flag == 1; }, function () {
                mysqlConnection.query("SELECT * FROM orders LIMIT " + offset + ",1000", function (err, rows, fields) {
                    if (err)
                        throw err;
                    else {
                        if (rows.length > 0) {
                            const params = [];
                            const chunks = [];
                            rows.forEach(row => {
                                const valueClause = [];
                                fields.forEach(field => {
                                    params.push(row[field.orgName]);
                                    valueClause.push('$' + params.length);
                                });
                                chunks.push('(' + valueClause.join(', ') + ')');
                            });
                            var pginsert = 'INSERT INTO superstore (' + fields.map(field => '"' + field.orgName.toLowerCase() + '"').join(',') + ') ' +
                                'VALUES ' + chunks.join(', ');
                            try {
                                pgQuery(clientpg, pginsert, params);
                            }
                            catch (err) {
                                console.error(`Error occured when offset ${offset}, moving on...`);
                            }
                            count += rows.length;
                            console.log("Running data " + count);
                        }
                        else {
                            // sudah tidak ada data
                            flag = 0;
                        }
                    }
                });
                offset += 1000;
                return Q.delay(0); // arbitrary async
            }).then(function () {
                if(skip==0)console.log("Done Input");
                else console.log("Already have data in Table superstore");
                
                
            });
        }
    });
}
load();