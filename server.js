const express = require('express');
const multer = require('multer');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const app = express();
const port = 3000;

// Set up PostgreSQL connection pool
const pool = new Pool({
  user: 'postgres',       // Your PostgreSQL user
  host: 'localhost',      // Database host (localhost for local connection)
  database: 'a-dw-proj',  // Your database name
  password: '1234',       // Your password
  port: 5432,             // Port (default PostgreSQL port)
});

// Set up Multer for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, './uploads');
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  },
});

const upload = multer({ storage });

// Create the uploads directory if it doesn't exist
if (!fs.existsSync('./uploads')) {
  fs.mkdirSync('./uploads');
}

// Serve the front-end UI (HTML, CSS, JS)
app.use(express.static('public'));

// Helper function to process CSV and insert into the database
const processCsvAndInsert = async (filePath) => {
  const results = [];
  let rowCount = 0;
  const batchSize = 1000;  // Define batch size for inserts

  return new Promise((resolve, reject) => {
    const readStream = fs.createReadStream(filePath).pipe(csv());

    readStream.on('data', (data) => {
      results.push(data);
      rowCount++;
    });

    readStream.on('end', async () => {
      console.log(`CSV Parsing Complete. Parsed ${rowCount} rows.`);

      const client = await pool.connect(); // Get a client from the pool
      let insertedCount = 0;

      try {
        // Loop through the parsed CSV records in batches
        for (let i = 0; i < results.length; i += batchSize) {
          const batch = results.slice(i, i + batchSize);

          // Build the insert query for the current batch
          const values = batch.map((row, index) => {
            const baseIndex = index * 6; // Base index for the placeholders
            return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6})`;
          }).join(', ');

          // Flatten the array of row values into a single array
          const queryParams = batch.flatMap(row => [
            row['Order ID'] || '',  // Empty string if the field is null
            row['Product'] || '',
            row['Quantity Ordered'] || '',
            row['Price Each'] || '',
            row['Order Date'] || '',
            row['Purchase Address'] || ''
          ]);

          // Construct the query
          const query = `
            INSERT INTO Sales_Landing("Order ID", "Product", "Quantity Ordered", "Price Each", "Order Date", "Purchase Address")
            VALUES ${values}
          `;

          // Execute the query for the current batch
          await client.query(query, queryParams);
          insertedCount += batch.length;

          // Log progress for every 1000 rows
          if (insertedCount % 1000 === 0) {
            console.log(`Inserted ${insertedCount} records...`);
          }
        }

        console.log(`CSV processing complete. ${insertedCount} records inserted into the database.`);
        resolve();  // Resolve the promise once the insert is complete
      } catch (error) {
        console.error('Error inserting data into Sales_Landing:', error);
        reject(error);  // Reject the promise on error
      } finally {
        client.release();  // Release the client back to the pool
        fs.unlinkSync(filePath); // Optionally, delete the uploaded file after processing
      }
    });

    readStream.on('error', (error) => {
      reject(error); // Reject the promise on stream error
    });
  });
};

// Endpoint for handling multiple file upload and CSV parsing
app.post('/upload', upload.array('files', 10), async (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ success: false, message: 'No files uploaded' });
  }

  try {
    // Process each uploaded file sequentially
    for (const file of req.files) {
      const filePath = path.join(__dirname, 'uploads', file.filename);
      console.log(`Starting to process file: ${filePath}`);
      
      // Process and insert the current CSV file
      await processCsvAndInsert(filePath);
    }

    // Respond once all files have been successfully processed
    res.json({ success: true, message: 'Files uploaded and processed successfully' });
  } catch (error) {
    console.error('Error during file processing:', error);
    res.status(500).json({ success: false, message: 'Error processing CSV files' });
  }
});

// Start the Express server
app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
