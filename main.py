# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import pymongo


# ------------------------------------------
# FUNCTION 1: most_popular_cuisine
# ------------------------------------------
def most_popular_cuisine(db):
    # 1. Create the pipeline of actions to query the collection
    pipeline1 = []

    # 1.1. Group the restaurants by their cuisine style, counting how many are there per group
    command_1 = {"$group": {"_id": {"Cuisine": "$cuisine"}, "total": {"$sum": 1}}}
    pipeline1.append(command_1)

    # 1.2. Sort the documents by decreasing order
    command_2 = {"$sort": {"total": -1}}
    pipeline1.append(command_2)

    # 1.3. Filter the documents so as to get just the first document
    command_3 = {"$limit": 1}
    pipeline1.append(command_3)

    # 1.4. Trigger the query to the MongoDB and convert the result to a list of documents
    popular_cuisines = db.restaurants.aggregate(pipeline1)

    # Set string equal to "_id" which is {'Cuisine': 'American '}
    cuisine_type = ""
    for c in popular_cuisines:
        cuisine_type = c["_id"]
    print("Cuisine type", cuisine_type)

    popular_cuisines = cuisine_type

    # 2. Get the total amount of restaurants
    pipeline2 = []
    command_a = {"$group" : { "_id" : "restaurant_id", "count" : { "$sum" : 1 } } }
    pipeline2.append(command_a)
    total_rest = db.restaurants.aggregate(pipeline2)

    # Set int equal to "count" which is 50718
    total_num_rest = 0
    for r in total_rest:
        total_num_rest = r["count"]
    print("Total rest:", total_num_rest)

    # 3. Extract the name of the cuisine we are looking for (and its percentage of restaurants)
    most_popular = popular_cuisines
    total = 0

    # 4. Return this cuisine name
    return most_popular, total

# ------------------------------------------
# FUNCTION 2: ratio_per_borough_and_cuisine
# ------------------------------------------
def ratio_per_borough_and_cuisine(db, cuisine):
    # 1. First pipeline: Query the collection to get how many restaurants are there per borough
    pipeline1 = []
    command_1 = {"$group" : { "_id" : "$borough", "count" : { "$sum" : 1 } } }, { "$sort" : { "count" : -1 }}
    pipeline1.append(command_1)


    # 2. Second pipeline: Query the collection to get how many restaurants (of the kind of cuisine we are looking for) are there per borough
    pipeline2 = []
    command_2 = { "$project" : { "_id" : 0, "Borough" : "$borough", "Cuisine" : {"$cond" : [ {"$eq" : ["$cuisine", "American " ] }, 1, 0]} } }, \
                { "$group" : { "_id" : "$Borough", "count" : { "$sum" : "$Cuisine" } } }, { "$sort" : { "count" : -1 } }
    pipeline2.append(command_2)


    # 3. Combine the results of the two queries, so as to get the ratio of restaurants (of the kind of cuisine we are looking for) per borough
    # Plese note that the documents of first and second query might not fully match. That is, it might be the unlikely case in which, for one of the boroughs, there is no restaurant (of this kind of cuisine we are looking for) at all.

    # 4. Select the name and ratio of the borough with smaller ratio
    name = ""
    ratio = 100.0


    # 5. Return the selected borough and its ratio
    return (name, ratio)

# ------------------------------------------
# FUNCTION 3: ratio_per_zipcode
# ------------------------------------------
def ratio_per_zipcode(db, cuisine, borough):
    # 1. First pipeline: Query the collection to get the biggest five zipcodes of the borough (in which we are going to open the new restaurant)
    pipeline1 = []


    # 2. Second pipeline: Query the collection to get how many zipcodes of the borough include restaurants of the kind of cuisine we are looking for
    pipeline2 = []


    # 3. Combine the results of the two queries, so as to compute the ratio of restaurants (of our kind of cuisine) for each of the 5 biggests zipcodes of the borough
    # Plese note that the documents of first and second query might not fully match. That is, there might be more than 5 zipcodes in the second query. Also, it might be the unlikely case in which, for one of the biggest zipcodes of the borough, there is no restaurant (of the kind of cuisine we are looking for) at all.


    # 4. Extract the name of the cuisine we are looking for (and its percentage of restaurants)
    name = ""
    ratio = 100.0


    # 5. Return the selected borough and its ratio
    return (name, ratio)

# ------------------------------------------
# FUNCTION 4: best_restaurants
# ------------------------------------------
def best_restaurants(db, cuisine, borough, zipcode):
    # 1. First pipeline: Query the collection to get the three restaurants of this borough, zipcode and kind of cuisine with better average review.
    # Filter the restaurants to consider only these ones with more than 4 or more reviews.
    pipeline1 = []


    # 2. Format the result to a list of pairs
    name = []
    reviews = []


    # 3. Return the selected restaurant names and average review scores
    return name, reviews

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------/
def my_main():
    # 0. We set up the connection to the cluster
    client = pymongo.MongoClient('localhost', 27000)
    db = client.test

    # pipeline1 = []
    # pipeline1.append({"$group": {"_id": {"cuisine": "$cuisine"}, "total": {"$sum": 1}}})
    # pipeline1.append({"$sort" : { "total" : -1 } })
    # pipeline1.append({ "$limit" : 1 })
    # res = list(db.restaurants.aggregate(pipeline1))
    # print(res)



    # 1. What is the kind of cuisine with more restaurants in the city?
    (cuisine, ratio_cuisine) = most_popular_cuisine(db)
    print ("1. The kind of cuisine with more restaurants in the city is", cuisine, "(with a", ratio_cuisine, "percentage of restaurants of the city)")
    #
    # # 2. Which is the borough with smaller ratio of restaurants of this kind of cuisine?
    # (borough, ratio_borough) = ratio_per_borough_and_cuisine(db, cuisine)
    # print ("2. The borough with smaller ratio of restaurants of this kind of cuisine is", borough, "(with a", ratio_borough, "percentage of restaurants of this kind)")
    #
    # # 3. Which of the 5 biggest zipcodes of the borough has a smaller ratio of restaurants of the cuisine we are looking for?
    # (zipcode, ratio_zipcode) = ratio_per_zipcode(db, cuisine, borough)
    # print ("3. The zipcode of the borough with smaller ratio of restaurants of this kind of cuisine is zipcode =", zipcode, "(with a", ratio_zipcode, "percentage of restaurants of this kind)")
    #
    # # 4. Which are the best 3 restaurants (of the kind of cuisine we are looking for) of our zipcode?
    # (best, reviews) = best_restaurants(db, cuisine, borough, zipcode)
    # # print ("4. The best three restaurants (of this kind of couisine) at these zipcode are:", best[0], "(with average reviews score of", reviews[0], "),", best[1], "(with average reviews score of", reviews[1], "),", best[2], "(with average reviews score of", reviews[2], ")")

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    my_main()

