const express = require('express');
const { MongoClient } = require('mongodb');
const bodyParser = require('body-parser');
const csv = require('csv-parser');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

const app = express();
const port = process.env.PORT || 3000;
const mongoUrl = 'mongodb+srv://kriswantomhc:admin@rdbstatus.nc0g85g.mongodb.net/';

app.use(bodyParser.json());

MongoClient.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(client => {
    const db = client.db('RDB_status_update');
    const collection = db.collection('RDB');

    app.post('/insert', (req, res) => {
      collection.insertOne(req.body)
        .then(result => res.status(201).send(result))
        .catch(error => console.error(error));
    });

    app.get('/fetch-and-store-csv', async (req, res) => {
        const csvUrl = 'https://meliteh-api.azurewebsites.net/rdb-api/r83kant3l/all_applicants.csv?start_time=2024-01-01T00:00:00';
        
        try {
            await client.connect();
    
            // Step 1: Fetch and parse CSV data
            const response = await fetch(csvUrl);
            const csvData = [];
            await new Promise((resolve, reject) => {
                response.body.pipe(csv())
                    .on('data', (data) => csvData.push(data))
                    .on('end', resolve)
                    .on('error', reject);
            });
    
            // Step 2: Get all existing data from MongoDB
            const existingData = await collection.find({}).toArray();
            const existingMap = new Map(existingData.map(item => [`${item.PersonID}-${item.Status}-${item.StatusDate}`, true]));
    
            // Step 3: Compare and filter out duplicates
            const newData = csvData.filter(item => !existingMap.has(`${item.PersonID}-${item.Status}-${item.StatusDate}`));
    
            // Insert non-duplicate data into MongoDB
            if (newData.length > 0) {
                await collection.insertMany(newData);
                res.send(`Inserted ${newData.length} new records.`);
            } else {
                res.send('No new records to insert.');
            }
        } catch (error) {
            console.error('Error:', error);
            res.status(500).send('Failed to fetch and store CSV data.');
        } finally {
            await client.close();
        }
    });      

    app.delete('/delete-rdb', async (req, res) => {
        try {
          await client.connect();
          const result = await collection.deleteMany({}); // Empty filter to match all documents
          res.json({ message: `Successfully deleted ${result.deletedCount} documents.` });
        } catch (error) {
          console.error("Error deleting documents:", error);
          res.status(500).send("Error deleting documents.");
        } finally {
          await client.close();
        }
      });

    app.listen(port, () => {
      console.log(`Listening on http://localhost:${port}`);
    });
  })
  .catch(console.error);
