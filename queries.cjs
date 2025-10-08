const { MongoClient } = require('mongodb');

const uri = 'mongodb+srv://kinyadoreen01_db_user:Tsl0Rl6492HsZant@cluster0.25yiemg.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB server");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Task 2: Basic CRUD Operations

    // Find all books by George Orwell
    const orwellBooks = await collection.find({ author: "George Orwell" }).toArray();
    console.log("Books by George Orwell:", orwellBooks);

    // Count books in each genre
    const genreCounts = await collection.aggregate([
      { $group: { _id: "$genre", count: { $sum: 1 } } }
    ]).toArray();
    console.log("Genre Counts:", genreCounts);

    // Update price of "The Great Gatsby" to 14.99
    const updateResult = await collection.updateOne(
      { title: "The Great Gatsby" },
      { $set: { price: 14.99 } }
    );
    console.log("Updated ${updateResult.modifiedCount} document(s)");

    // Delete the book "Animal Farm"
    const deleteResult = await collection.deleteOne({ title: "Animal Farm" });
    console.log("Deleted ${deleteResult.deletedCount} document(s)");

    // Task 3: Advanced Queries

    // Find books in stock and published after 2010
    const booksInStock = await collection.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray();
    console.log("Books in stock published after 2010:", booksInStock);

    // Projection: title, author, price
    const booksWithProjection = await collection.find({}, { projection: { title: 1, author: 1, price: 1 } }).toArray();
    console.log("Books with selected fields:", booksWithProjection);

    // Sort books by price ascending
    const sortedBooksAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log("Books sorted by price ascending:", sortedBooksAsc);

    // Pagination: first 5 books
    const paginatedBooks = await collection.find().skip(0).limit(5).toArray();
    console.log("Paginated books (page 1):", paginatedBooks);

    // Task 4: Aggregation Pipeline

    // Average price by genre
    const averagePriceByGenre = await collection.aggregate([
      { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } }
    ]).toArray();
    console.log("Average price by genre:", averagePriceByGenre);

    // Author with most books
    const authorWithMostBooks = await collection.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log("Author with the most books:", authorWithMostBooks);

    // Group books by publication decade and count
    const booksByDecade = await collection.aggregate([
  {
    $project: {
      decade: {
        $multiply: [
          { $floor: { $divide: ["$published_year", 10] } },
          10
        ]
      }
    }
  },
  {
    $group: {
      _id: "$decade",
      count: { $sum: 1 }
    }
  },
  { $sort: { _id: 1 } }
]).toArray();

console.log("Books grouped by decade:", booksByDecade);

    // Task 5: Indexing
    // Create an index on the title field
    await collection.createIndex({ title: 1 });
    console.log("Index created on title field");

    // Create a compound index on author and published_year fields
    await collection.createIndex({ author: 1, published_year: 1 });
    console.log("Compound index created on author and published_year fields");

  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
  }
}

runQueries().catch(console.dir);
