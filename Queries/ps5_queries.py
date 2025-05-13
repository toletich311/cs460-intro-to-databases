#
# CS 460: Problem Set 5: MongoDB Query Problems
#

#
# For each query, use a text editor to add the appropriate XQuery
# command between the triple quotes provided for that query's variable.
#
# For example, here is how you would include a query that finds
# the names of all movies in the database from 1990.
#
sample = """
    db.movies.find( { year: 1990 }, 
                    { name: 1, _id: 0 } )
"""

#
# 1. Put your query for this problem between the triple quotes found below.
#    Follow the same format as the model query shown above.
#
query1 = """

db.people.find( { name: /Joy/},
                { _id:0, name: 1, dob:1 }
              )

"""

#
# 2. Put your query for this problem between the triple quotes found below.
#
query2 = """

db.movies.find({ $or: [ { name: "The Holdovers"},
			{ name: "Spotlight" }
		      ],
		}, 
		{_id:0, name: 1, runtime:1}
	       )

"""

#
# 3. Put your query for this problem between the triple quotes found below.
#
query3 = """

db.movies.count( {$and: [{"actors.name": /Robert Downey Jr./}, 
                        {earnings_rank: { $exists: true }}
                       ]}
)

"""

#
# 4. Put your query for this problem between the triple quotes found below.
#
query4 = """

db.movies.aggregate(	{ $match: { earnings_rank: {$lte: 200} } },
			{ $group: { _id: "$rating", count: { $sum: 1 },best: { $min: "$earnings_rank" } } },
			{ $match: { count: { $gte: 1 } } },
			{ $project: { _id: 0, rating: "$_id", num_top_grossers: "$count",  best_rank: "$best"  } } 
  )

"""

#
# 5. Put your query for this problem between the triple quotes found below.
#
query5 = """

db.oscars.distinct("movie.name", { $and: [ { year: { $gte: 2010 } },{ year: { $lte: 2019 } } ]})

"""

#
# 6. Put your query for this problem between the triple quotes found below.
#
query6 = """

db.people.aggregate( 	{$match: {pob: /England, UK$/}},
			{$sort: {dob:-1}},
			{$project: {_id:0, name: 1, dob:1, pob:1}},
			{$limit: 3}

		)

"""

#
# 7. Put your query for this problem between the triple quotes found below.
#
query7 = """

db.movies.aggregate(    
            {$match: {$and: [{genre: /A/},{earnings_rank: {$gte:1}}, {earnings_rank: {$lte:15}}]}},
            {$group: { _id: null, count: { $sum: 1 }, avg: {$avg: "$earnings_rank"}, min: {$min: "$earnings_rank"} }},
            {$project: {_id:0, num_action_in_top_15: "$count",avg_ranking: "$avg" ,best_rank: "$min"}}

)

"""

#
# 8. Put your query for this problem between the triple quotes found below.
#
query8 = """

db.movies.aggregate ( 	{$match: {genre: /A/}},
            {$unwind:  "$directors"},
			{$group: {_id: "$directors.name", count: {$sum: 1}}},
			{$sort: {count: -1}},
			{$limit: 3},
			{$project: {_id:0, director: "$_id", num_action: "$count"}}

)

"""

#
# 9. Put your query for this problem between the triple quotes found below.
#
query9 = """

db.oscars.aggregate (	{$match: {person: {$exists: true }}},
			{$unwind:  "$person"},
			{$group: {_id: "$person.name", count: {$sum: 1}, types: {$addToSet: "$type"}}},
			{$match: {count: { $gte: 3 }}},
            {$sort:  {count: -1, _id: 1}},
			{$project: {_id:0, num_awards: "$count", oscar_winner: "$_id", types: "$types"}}


)

"""

#
# 10. Put your query for this problem between the triple quotes found below.
#
query10 = """

db.movies.aggregate (	{$unwind:  "$actors"},
			{$group: {_id: "$actors.name", num_movies: {$sum: 1}, max: {$max: "$year"}, min: {$min: "$year"}, movies: {$addToSet: "$name"} }},
			{$match: {num_movies: { $gte: 10 }}},
            {$sort: {_id: 1}},
			{$project: {_id:0, actor: "$_id", last_appeared: "$max", first_appeared: "$min", movies: "$movies"}}
			
)

"""
