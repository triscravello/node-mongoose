const connect = require("./db");
const { ObjectId } = require('mongodb');

const runDatabaseQueries = async () => {
  
  const db = await connect();
  const movies = db.collection('movies');

  // Create a new user document to the users collection. Include fields name and email.
  const users = db.collection('users');
  const newUser = { name: "Becca Rutila", email: "becca_rutila" + Math.floor(Math.random() * 1000) + "@example.com" };
  const insertResult = await users.insertOne(newUser);
  console.log('Inserted User:', insertResult.insertedId);
  console.log('Inserted User:', { name: newUser.name, email: newUser.email });

  // Read: Find all movies directed by Christopher Nolan.
  const nolanMovies = await movies.find({ directors: "Christopher Nolan" }).toArray();
  console.log('Christopher Nolan Movies:', nolanMovies);

  // Find movies that include the genre "Action" and sort descending by year.
  const actionMovies = await movies.find({ genres: "Action" }).sort({ year: -1 }).toArray();
  console.log('Action Movies:', actionMovies);

  // Find movies with an IMDb rating greater than 8 and return only the title and IMDB information.
  const highRatedMovies = await movies.find({ "imdb.rating": { $gt: 8 }})
    .project({ title: 1, imdb: 1, _id: 0 })
    .toArray();
  console.log('High Rated Movies:', highRatedMovies);

  // Find movies that starred both "Tom Hanks" and "Tim Allen".
  const tomAndTimMovies = await movies.find({ cast: { $all: ["Tom Hanks", "Tim Allen"] }}).toArray();
  console.log('Movies with Tom Hanks and Tim Allen:', tomAndTimMovies);

  // Find movies that starred both and only "Tom Hanks" and "Tim Allen".
  const onlyTomAndTimMovies = await movies.find({ cast: { $all: ["Tom Hanks", "Tim Allen"], $size: 2 }}).toArray();
  console.log('Movies with only Tom Hanks and Tim Allen:', onlyTomAndTimMovies);

  // Find comedy movies that are directed by Steven Spielberg.
  const spielbergComedies = await movies.find({ genres: "Comedy", directors: "Steven Spielberg" }).toArray();
  console.log('Comedy Movies directed by Steven Spielberg:', spielbergComedies);

  // Update: Add a new field "available_on" with the value "Sflix" to "The Matrix"
  const updatedResult = await movies.updateOne(
    { title: "The Matrix" },
    { $set: { available_on: "Sflix" } }
  );
  console.log('Updated The Matrix:', updatedResult.modifiedCount === 1 ? 'Success' : 'Failed');

  // Increment the metacritic of "The Matrix" by 1
  const incrementResult = await movies.updateOne(
    { title: "The Matrix" },
    { $inc: { metacritic: 1 } }
  );
  console.log('Incremented The Matrix Metacritic:', incrementResult.modifiedCount === 1 ? 'Success' : 'Failed');

  // Add a new genre "Gen Z" to all movies released in the year 1997
  const addGenreResult = await movies.updateMany(
    { year: 1997 },
    { $addToSet: { genres: "Gen Z" } }
  );
  console.log('Added "Gen Z" genre to 1997 movies:', addGenreResult.modifiedCount);

  // Increase IMDb rating by 1 for all movies with a rating less than 5.
  const increaseRatingResult = await movies.updateMany(
    { "imdb.rating": { $lt: 5} },
    { $inc: { "imdb.rating": 1 } }
  );
  console.log('Increased IMDb rating for movies with rating < 5:', increaseRatingResult.modifiedCount);

  // Delete: Delete a comment with a specific ID.
  const comments = db.collection('comments');
  const commentIdToDelete = "573a1393f29313caabcd9e5b";
  const deleteCommentResult = await comments.deleteOne({ _id: new ObjectId(commentIdToDelete) });
  console.log('Deleted Comment:', deleteCommentResult.deletedCount === 1 ? 'Success' : 'Failed');

  // Delete all comments made for "The Matrix"
  const deleteMatrixCommentsResult = await comments.deleteMany({ movie_id: new ObjectId("573a1393f29313caabcd9e5b") });
  console.log('Deleted Comments for The Matrix:', deleteMatrixCommentsResult.deletedCount);

  // Delete all movies that do not have any genres.
  const deleteNoGenreMoviesResult = await movies.deleteMany({ genres: { $exists: true, $size: 0 } });
  console.log('Deleted Movies with No Genres:', deleteNoGenreMoviesResult.deletedCount);

  // Aggregate: Aggregate movies to count how many were released each year and display from the earliest year to the latest. 
  const moviesPerYear = await movies.aggregate([
    { $group: { _id: "$year", count: { $sum: 1 } } },
    { $sort: { _id: 1 } } // Sort by year ascending
  ]).toArray();
  console.log("Movies released per year:");
  moviesPerYear.forEach(doc => {
    console.log(`Year: ${doc._id}, Count: ${doc.count}`);
  });

  // Calculate the average IMDb rating for movies grouped by director and display from highest to lowest.
  const avgRatingByDirector = await movies.aggregate([
    { $match: { "imdb.rating": { $ne: null } } }, // Only include movies that have a rating
    { $unwind: "$directors" }, // In case 'directors' is an array
    { $group: {
      _id: "$directors",
      averageRating: { $avg: "$imdb.rating" },
      movieCount: { $sum: 1 }
    }},
    { $sort: { averageRating: -1 } }, // Highest rating first 
  ]).toArray();
  console.log("Average IMDb rating by director:");
  avgRatingByDirector.forEach(doc => {
    console.log(`Director: ${doc._id}, Avg Rating: ${doc.averageRating.toFixed(2)}, Movies: ${doc.movieCount}`);
  });

  // Run this query, should get top 5 best rated movies on IMDB
  //const topMovies = await movies.find({ "imdb.rating": { $gt: 8.0 } })
   // .project({ title: 1, year: 1, "imdb.rating": 1 })
   // .sort({ "imdb.rating": -1 })
   // .limit(5)
   // .toArray();

  //console.log('Top Rated Movies:', topMovies);

  process.exit(0);
};


runDatabaseQueries();