const express = require("express");
const app = express();
const port = process.env.PORT || 8081;
const bodyParser = require("body-parser");

//db connection
const MongoClient = require('mongodb').MongoClient;
const url = "mongodb+srv://challengeUser:WUMglwNBaydH8Yvu@challenge-xzwqd.mongodb.net/getir-case-study?retryWrites=true";

//middleware definitions
app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());
app.use((req, res, next) => {
    console.log(`${req.path} urline ${req.method} istek atılıyor...`);
    next();
});

function dbCall(request) {
    return new Promise(function (resolve, reject) {
        let startDate = request.startDate;
        let endDate = request.endDate;
        let minCount = request.minCount;
        let maxCount = request.maxCount;
        MongoClient.connect(url, (err, client) => {
            if (err) throw err;
            const db = client.db('getir-case-study');
            var pipeline = [
                {
                    "$match": {
                        "createdAt": {"$gte": new Date(startDate), "$lte": new Date(endDate)}
                    }
                },
                {
                    $unwind: "$counts"
                },
                {
                    "$group": {
                        "_id": "$_id",
                        "totalCount": {"$sum": "$counts"},
                        "key": {$first: '$key'},
                        "createdAt": {$first: '$createdAt'},
                    }
                },
                {
                    "$match": {
                        "totalCount": {"$gte": minCount, "$lte": maxCount}
                    }
                },
            ]
            db.collection('records').aggregate(pipeline).toArray((err, result) => {
                if (err) reject(err);
                client.close();
                return resolve(result);
            });
        });
    })

}

//Date format validation
function isValidDate(dateString) {
    console.log(dateString)
    let regEx = /^\d{4}-\d{2}-\d{2}$/;
    return dateString.match(regEx) != null;
}

//request field validation
function requestValidation(req) {
    console.log(req)
    if (!req.startDate) {
        return {
            code: "1",
            msg: "startDate not defined"
        }
    }
    if (!req.endDate) {
        return {
            code: "2",
            msg: "endDate not defined"
        }
    }
    if (!req.minCount) {
        return {
            code: "3",
            msg: "minCount not defined"
        }
    }
    if (!req.maxCount) {
        return {
            code: "4",
            msg: "maxCount not defined"
        }
    }
    if (!isValidDate(req.startDate)) {
        return {
            code: "5",
            msg: "startDate format is not defined. Proper format is YYYY-MM-DD"
        }
    }
    if (!isValidDate(req.endDate)) {
        return {
            code: "6",
            msg: "endDate format is not defined. Proper format is YYYY-MM-DD"
        }
    }
    return {
        code: "0",
        msg: "Success"
    }
}

//Post method
app.post("/api/test", async (req, res, next) => {
    const validation = requestValidation(req.body);
    if (validation.code != 0) {
        res.json({
            code: validation.code,
            msg: validation.msg
        });
    } else {
        let response = await dbCall(req.body);
        console.log(response);
        res.json({
            code: validation.code,
            msg: validation.msg,
            records: response.map(item => {
                return ({
                    key: item.key,
                    createdAt: item.createdAt,
                    totalCount: item.totalCount
                })
            })
        });
    }

});


app.listen(port, () => {
    console.log(`localhost:${port} -> Api is alive ! `);
});
