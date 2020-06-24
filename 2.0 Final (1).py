# Databricks notebook source
# MAGIC %md # Are Elite Yelp Users More Indicative of the Crowd than Non-Elite Users?
# MAGIC ##Does it vary geographically or by top level category?
# MAGIC 
# MAGIC ### By Andrew Kehl

# COMMAND ----------

# MAGIC %md #But Why?
# MAGIC 
# MAGIC The world would probably be a better place if more people asked this question before taking an action. Especially when they take half a semester working on a project. We ask this question as if we are Yelp. Why might Yelp want to have an idea of if elite users are more indicative of the crowd than non-elite users? Well, Yelp places a lot of responsibility on their elite users. They weigh elite users' reviews heavier in their proprietary sorting algorithm. And by tagging a user as elite they are telling anybody who might be looking at Yelp reviews that this person is more knowledgeable and consistent than another random non-elite user. With all this trust, Yelp better be quite sure that Yelp elite users really are more indicative of the crowd than non-elite users. 
# MAGIC 
# MAGIC To begin we must first define what "indicative of the crowd" really means. Today we are going to define it as meaning: 'On average, will an elite user's review star rating align more closely to a business's mean review rating than a non-elite users?'

# COMMAND ----------

# MAGIC %md #Dataset Introduction
# MAGIC 
# MAGIC 
# MAGIC We are going to begin our adventure by taking a look at our data. For purposes of gaining some cheap insight and adding to the repertoire of student datasets available, Yelp has given us access to some of their data. 
# MAGIC 
# MAGIC Specifically Yelp has given us access to five JSON files.
# MAGIC 
# MAGIC 1. business.json gives us things like each business name, location,categorical data, hours, and business attributes.
# MAGIC 
# MAGIC 2. review.json gives us all the data tied to each review. This is everything about the review such as which business it is for, what the star rating for the business is, the review text, the reviewer's user id and the date of the review.
# MAGIC 
# MAGIC 3. user.json gives us everything Yelp has about each user in the dataset. These data points include how many reviews this user made, the person's name, the years this user has been considered elite, this user's average star rating, when this person joined Yelp, and other attributes that are probably not relevant to us today.
# MAGIC 
# MAGIC 4. checkin.json tracks some small non review tidbits users have provided. We won't go in depth here because it isn't particularly relevant to answering our question.
# MAGIC 
# MAGIC 5. categories.json this is a data frame that has all top level categories and subcategory combinations.
# MAGIC 
# MAGIC 6. tip.json again tracks tips users have given on businesses but without review data attached. We won't go further in depth here because we won't be using it.
# MAGIC 
# MAGIC 7. photo.json gives us the metadata for each photo in the reviews. Again, we will ignore this file.

# COMMAND ----------

# MAGIC %md ## Dataset Continued
# MAGIC 
# MAGIC Yelp obviously isn't going to give all of their data away for free but what they do give us is very complete. Where it is limited is in geographic area. The data comes from 10 distinct metropolitan areas throughout the country. We will explore what metro areas these are a bit later in our adventure. 
# MAGIC 
# MAGIC 
# MAGIC You can read more about the dataset here:
# MAGIC 
# MAGIC 
# MAGIC <a href="https://www.yelp.com/dataset/documentation/main" target="_blank">Yelp Dataset Documentation</a>
# MAGIC 
# MAGIC <a href="https://www.yelp.com/dataset/download" target="_blank">Yelp Dataset Download Form</a>

# COMMAND ----------

# MAGIC %md # Reading, Wrangling, Cleaning, and Classifying

# COMMAND ----------

# MAGIC %md ##Reading in review.json and wrangling it
# MAGIC To begin we will start by importing the select few columns from our review data. We are bringing five columns:
# MAGIC 
# MAGIC "review_id" -  This is just a unique identifier for each review.
# MAGIC 
# MAGIC "user_id" -  This is each user's unique identifier, that way we can tie each review back to whomever wrote it.
# MAGIC 
# MAGIC "stars" -  This is the star rating the user gave to this review.
# MAGIC 
# MAGIC "business_id"  - This is the business's unique identifier.
# MAGIC 
# MAGIC "date" -  This is the dat which the review was written

# COMMAND ----------

df_reviews = spark.read.json('/yelpdata/review.bz2').select("review_id","user_id","stars","business_id","date").cache()#Read review.json into a dataframe
print("review count:", df_reviews.count() )
df_reviews.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC Just below in command 9 essentially the only thing done here is the year is pulled out of the date column, assigned to a new column in the array format called 'review_year'. We are assigning it to any array so that later in the notebook we can use the array_intersect function. After, the date column is dropped and we only have the year column.

# COMMAND ----------

import pyspark.sql.functions as f #import some sql functions
df_reviews = df_reviews.select("*", f.split(df_reviews.date,'-').alias('review_year'))#turn date string into a list called review_year, still includes the day, month, time
df_reviews=df_reviews.withColumn("review_year",df_reviews.review_year[0])# Pulls just the year from the array and assigns it back to review_year
df_reviews = df_reviews.withColumn("year", f.array(f.col("review_year")))#Creates a new column called year that is an array contains only the review year, that way we can use array_intersect later
df_reviews=df_reviews.select("review_id","user_id","stars","business_id","year")#calling out only the columns we need
df_reviews.cache()

# COMMAND ----------

# MAGIC %md ## Read in user data and wrangling
# MAGIC 
# MAGIC Next, we bring in the user.json and only choose the "user_id" and "elite" clomuns.
# MAGIC 
# MAGIC "user_id"-This is self explanatory, a user's unique identifier
# MAGIC 
# MAGIC "elite" -This column is a comma seperated list with all of the years the user is elite. 

# COMMAND ----------

df_users = spark.read.json('/yelpdata/user.bz2').select("user_id","elite",)#Read user.json into a dataframe
df_users.cache
print("user count:", df_users.count() )
df_users.printSchema()

# COMMAND ----------

# MAGIC %md Next we will turn the column seprated list in the elite column and turn it into an array while renaming it "elite_years". Then we will create temporary views for both the user and review data so that we can call them in SQL.

# COMMAND ----------

df_users=df_users.select("*", f.split(df_users.elite,'\s*,\s*').alias("elite_years"))#turn the elite column into an array called elite_years
df_users=df_users.select("user_id","elite_years")#dump everything except user_id and elite_years
df_users.createOrReplaceTempView("users")#create temp view
df_reviews.createOrReplaceTempView("reviews")#create temp view


# COMMAND ----------

# MAGIC %md ##Joining user and review data
# MAGIC 
# MAGIC 
# MAGIC We are going to do a left join of the review and user data on the only common unique identifier between them: user_id. The only column we are adding to the review data is the years the reviewer was elite.

# COMMAND ----------

DFuser_reviews=spark.sql("""
SELECT R.user_id,R.review_id,R.business_id,R.stars,R.year,U.elite_years
FROM reviews as R LEFT JOIN users as U
ON R.user_id=U.user_id
""")# This function joins all the user data and tags it to each review for further processing

# COMMAND ----------

# MAGIC %md ## Check to see if a review is from an elite user
# MAGIC 
# MAGIC 
# MAGIC As we said earlier we made the elite_years and the review years columns an array so we could use the array_intersect function. We are using that function here to check whether or not the user was elite when they wrote the review. We are assiging that check to a new colum called "Is_elite". If the users was elite when they wrote the review then the column includes the year, if they weren't elite the value will be null.

# COMMAND ----------

from pyspark.sql.functions import array_intersect #imports array_intersect function
DFuser_reviews=DFuser_reviews.withColumn("is_elite",array_intersect(DFuser_reviews.year,DFuser_reviews.elite_years))#This checks if the user was elite when the review was written.
DFuser_reviews=DFuser_reviews.select("is_elite","stars","business_id","review_id","user_id")#dumps the rest of the excess columns
DFuser_reviews.cache()
df_reviews.unpersist()
df_users.unpersist()
DFuser_reviews.createOrReplaceTempView("userreviews")#create temp view

# COMMAND ----------

# MAGIC %md  ## Read in business data
# MAGIC 
# MAGIC This is just reading the business.json into a dataframe, we are only bringing in the business_id, state, and categories columns. State and business_id are self explanatory but the categories column is an array of strings of business categories. We then save it as a table so we can take a look at it in Tableau.

# COMMAND ----------

df_business = spark.read.json('/yelpdata/business.bz2').select("business_id","state","categories") #Read business.json into a dataframe
df_business.cache()
print("business count:", df_business.count() )
df_business.printSchema()
df_business.write.mode("overwrite").saveAsTable("yelp_bus")

# COMMAND ----------

# MAGIC %md ##Classifying metro areas visually
# MAGIC 
# MAGIC Lets see if there is any way to easily classify out these 10 metro areas. We are going to pull the data into Yelp, take a snapshot and explore it here. Command 21 and 22 are some borrowed code to display pictures in this notebook.

# COMMAND ----------

from PIL import Image

def getWidth(path):
  with Image.open(path) as img:
    width, height = img.size
    return(width)

# COMMAND ----------

import base64
def showimage(path, width=0):
  image_string = ""
  img_tag = ""
  # Get the base64 string for the image
  with open(path, "rb") as image_file:
    image_string = base64.b64encode(image_file.read() ).decode('utf-8') 
    
  # Is the width setting a positive integer?  A width of 50 means 50%
  if width > 0 and width < 1:
    print("If the width parameter is specified, it must be 1 or more.  A width of 50 means 50%. The width entered was " + str(width) + ", so the original image width was used.")
    width = 0 #reset
    
  if width == 0:
    height = 0
    # Get the width and height of the image in pixels
    with Image.open(path) as img:
      width, height = img.size
      
    framewidth = width * 1.1
    # Build the image tag
    img_tag = '''
    <style>
    div {
      min-width: %ipx;
      max-width: %ipx;
    }
    </style>
    <div><img src="data:image/png;base64, %s"  style="width:%ipx;height=%ipx;" /></div>''' % (framewidth,framewidth,image_string, width, height)
    
  else: # a width was specified
    originalWidth = getWidth(path)
    imagewidth = int( width / 100.0 * originalWidth)
    framewidth = int( imagewidth * 1.1 )
    # Build the image tag
    img_tag = '''
    <style>
    div {
      min-width: %ipx;
      max-width: %ipx;
    }
    </style>
    <div><img src="data:image/png;base64, %s"  width="%ipx" height="auto"></div>''' % (framewidth,framewidth,image_string, imagewidth)
  return(img_tag)

# COMMAND ----------

displayHTML( showimage("/dbfs/FileStore/tables/Bus_map-1548d.png",50) )

# COMMAND ----------

# MAGIC %md We can see from the map that there are 10 discreet metro areas with very defined lines. These can easily be pulled out using state lines if we fix a few things.
# MAGIC 
# MAGIC From the picture, a bit of help from google maps, and the exploration we can figure out what the metro areas are. 
# MAGIC 
# MAGIC We have 
# MAGIC 
# MAGIC Las Vegas, Nevada
# MAGIC 
# MAGIC Phoenix/Scottdale, Arizona
# MAGIC 
# MAGIC Charlotte, North Carolina
# MAGIC 
# MAGIC Champaign, Illinois
# MAGIC 
# MAGIC Madison, Wisconsion
# MAGIC 
# MAGIC Calgary, Alberta
# MAGIC 
# MAGIC Cleveland/Akron, Ohio
# MAGIC 
# MAGIC Toronto, Ontario
# MAGIC 
# MAGIC Montreal, Quebec
# MAGIC 
# MAGIC Pittsburgh, Pennsylvania

# COMMAND ----------

# MAGIC %md ## Cleaning up the business data
# MAGIC 
# MAGIC We can see from the map that there are some data points from multiple states when the metro area is close to borders. Since we are using state codes to identify metro areas we need to fix this. The big one is Charlotte, on the border of North and South Carolina. But Montreal has some data points in New York and Vermount as well. The below commands will put those inconsistencies into the state where the metro area exists. We are using state as an identifier for the metro area

# COMMAND ----------

from pyspark.sql.functions import when, col
df_business=df_business.withColumn("metro_area", when(col("state")=="SC","NC")
                                   .when(col("state")=="NY","QC")
                                   .when(col("state")=="VT","QC")
                                   .otherwise(df_business.state))
# This block will create a new column called metro_area. The state will be assigned unless the state value is NY, VT, or SC. If one of those three values the metro area will be updated to the correct one.

# COMMAND ----------

# MAGIC %md We are just paring back the business data to only what we need, then creating temporary views for the business data and the review data.

# COMMAND ----------

df_business=df_business.select("business_id","metro_area","categories")#pair down to only what we need
df_business.createOrReplaceTempView("business")#create temp view
DFuser_reviews.createOrReplaceTempView("user_reviews")#create temp view

# COMMAND ----------

# MAGIC %md ## Joining the business data to the user data
# MAGIC 
# MAGIC This join gives us basically all of the information we could possible have on each review, including everything we want to know about the user who wrote the review and the business information for the review. We name this dataframe DF1

# COMMAND ----------

DF1=spark.sql("""
SELECT B.business_id,B.metro_area,B.categories,M.review_id,M.stars,M.Is_elite
FROM user_reviews AS M LEFT JOIN business AS B
ON B.business_id = M.business_id
""")# This statement joins all the business data to the review data on business_id
DF1.cache()
DF1.createOrReplaceTempView("one")#create temp view

# COMMAND ----------

# MAGIC %md # Exploratory Analysis

# COMMAND ----------

# MAGIC %md ## Preliminary look at the answering our question
# MAGIC 
# MAGIC Below, we aren't saving these dataframes but we just want to take a look at them. This is simply some exploratory data analysis but it will give us the first look at answering our question. We are looking at the number of total reviews, the average of all reviews, the number of elite review, the total number of elite reviews, the number of non-elite review and the average of non-elite reviews.

# COMMAND ----------

DF2=spark.sql("""
SELECT COUNT(review_id) AS Total_NUMBER_OF_REVIEWS,AVG(stars)
FROM user_reviews
""").show()

DF3=spark.sql("""
SELECT COUNT(review_id) AS ELITE_NUMBER_OF_REVIEWS,AVG(stars)
FROM user_reviews
WHERE Is_elite[0] IS NOT NULL 
""").show()

DF4=spark.sql("""
SELECT COUNT(review_id) AS NOT_ELITE_NUMBER_OF_REVIEWS,AVG(stars)
FROM user_reviews
WHERE Is_elite[0] IS NULL
""").show()

#This will give us the total number of reviews and the breakdown of elite and not-elite reviews as well as the mean for each of those 3

# COMMAND ----------

# MAGIC %md So we see that the average star rating for elite reviews is actually closer to the total average than looking at simply non elite reviews. Interestingly elite reviews on average have higher star ratings than all reviews and non-elite star averages are lower.
# MAGIC 
# MAGIC 
# MAGIC This is important and we can speculate why. Elite users write way more reviews per user than non elite users. For the most part, we know that elite users review the majority of businesses they interact with, while non elite users write more polarizinig reviews. It is common practice for non-elite users to only write reviews when they have a bad experience at a business. They want to 'get back' at a business for a perceived slight or poor customer service. The way they do this is to go online and write that business a bad review. The other side is true as well, when non-elite users have a great experience or a personal tie to a business they will always be sure to leave a five star review. 
# MAGIC 
# MAGIC While elite users review nearly every business they interact with.

# COMMAND ----------

# MAGIC %md ## Distribution of stars broken down by total, elite, and non-elite
# MAGIC 
# MAGIC Below we will explore the star distribution between these three groups by looking at histograms of star ratings.

# COMMAND ----------

DF5=spark.sql("""
SELECT COUNT(review_id) AS Total_Number_of_Reviews_, AVG(stars) AS Stars
FROM user_reviews
GROUP BY stars
ORDER BY stars
""")
DF5.show()# Elite star distribution

DF6=spark.sql("""
SELECT COUNT(review_id) AS ELITE_Number_of_Reviews_, AVG(stars) AS Stars
FROM user_reviews
WHERE Is_elite[0] IS NOT NULL
GROUP BY stars
ORDER BY stars
""")
DF6.show()# Elite star distribution

DF7=spark.sql("""
SELECT COUNT(review_id) AS NOT_ELITE_Number_of_Reviews, AVG(stars) AS Stars
FROM user_reviews
WHERE Is_elite[0] IS NULL
GROUP BY stars
ORDER BY stars
""")
DF7.show()# non-elite star distribution

DF5.write.mode("overwrite").saveAsTable("tabfive")
DF6.write.mode("overwrite").saveAsTable("tabsix")
DF7.write.mode("overwrite").saveAsTable("tabseven")

# COMMAND ----------

# MAGIC %md ##Visual distribution of stars between all users, elite users and non-elite users

# COMMAND ----------

displayHTML( showimage("/dbfs/FileStore/tables/total_dist-caf13.png",50) )

# COMMAND ----------

displayHTML( showimage("/dbfs/FileStore/tables/eliote_dist-67cb0.png",50) )

# COMMAND ----------

displayHTML( showimage("/dbfs/FileStore/tables/non_elite_distribution-0172f.png",50) )

# COMMAND ----------

# MAGIC %md It looks like the hypothesis regarding non-elite reviews is true. Non-elite users have more extreme review ratings while elite users are more concentrated close to the mean star rating we found earlier of 3.71. The mode of stars of non-elite and total reviews is  five stars while the mode of elite_reviews is four stars.
# MAGIC 
# MAGIC To summarize a bit, non-elite users tend to have more extreme reviews, while elite users tend to review closer to the overall mean.

# COMMAND ----------

# MAGIC %md #Just a bit more wrangling

# COMMAND ----------

# MAGIC %md ##Finding the mean star rating for each business
# MAGIC 
# MAGIC We are going to need this down a bit lower for our analysis. We are again breaking it down by whether or not the user is elite.

# COMMAND ----------

DF8=spark.sql("""
SELECT COUNT(review_id) AS elite_num_of_reviews,AVG(stars) AS elite_mean_stars,business_id
FROM user_reviews
WHERE Is_elite[0] IS NOT NULL
GROUP BY business_id
ORDER BY elite_num_of_reviews 
""")
DF8.createOrReplaceTempView("eight")#create temp view
DF8.show()

# COMMAND ----------

DF9=spark.sql("""
SELECT COUNT(review_id) AS non_elite_num_of_reviews,AVG(stars) AS non_elite_mean_stars,business_id
FROM user_reviews
WHERE Is_elite[0] IS NULL
GROUP BY business_id
ORDER BY non_elite_num_of_reviews 
""")
DF9.createOrReplaceTempView("nine")#create temp view
DF9.show()

# COMMAND ----------

DF10=spark.sql("""
SELECT COUNT(review_id) AS Num_of_reviews,AVG(stars) AS total_mean_stars,business_id
FROM user_reviews
GROUP BY business_id
ORDER BY Num_of_reviews
""")
DF10.cache()
DF10.createOrReplaceTempView("ten")#create temp view
DF10.show()


# COMMAND ----------

# MAGIC %md Above, we learned some interesting things about the data and have had a new problem come up. Some of these businesses have very few reviews from elite user, or even from non-elite users. The problem here is that with a business having such a low sample size we don't have a strong idea of what the true star rating of the business should be. If a business has an average review rating of one star but only has one review, is that business really terrible? Or did one person have a bad experience and take to the internet? We will need to filter our businesses like this to avoid skewing our results.

# COMMAND ----------

# MAGIC %md ##Joining the means back to DF1
# MAGIC 
# MAGIC Below, we are going to join all of the above queries into the same dataframe. Again we are going to need this data for analysis further down.

# COMMAND ----------

means_and_counts=spark.sql("""
SELECT E.business_id, E.elite_num_of_reviews,E.elite_mean_stars,N.non_elite_num_of_reviews,N.non_elite_mean_stars
FROM eight AS E LEFT JOIN nine as N
ON E.business_id = N.business_id
""")
means_and_counts.createOrReplaceTempView("means")#create temp view

# COMMAND ----------

means_and_counts=spark.sql("""
SELECT M.business_id, M.elite_num_of_reviews,M.elite_mean_stars,M.non_elite_num_of_reviews,M.non_elite_mean_stars, T.Num_of_reviews,T.total_mean_stars
FROM means as M LEFT JOIN ten as T
ON M.business_id = T.business_id
""")
means_and_counts.createOrReplaceTempView("means")#create temp view
means_and_counts.cache

# COMMAND ----------

checks=spark.sql("""
SELECT O.business_id,O.metro_area,O.categories,O.stars AS review_stars,O.Is_elite,O.review_id,T.total_mean_stars AS business_stars,T.elite_num_of_reviews,T.non_elite_num_of_reviews
FROM one as O LEFT JOIN means as T
ON O.business_id = T.business_id
""")
checks.cache

# COMMAND ----------

# MAGIC %md It may seem strange for now, but we have tied a businesses number of elite and non elite reviews to use each review, we will need this later.
# MAGIC 
# MAGIC You can see what the "checks" dataframe now looks like below.

# COMMAND ----------

checks.show(truncate= False)
checks.createOrReplaceTempView("checks")#create temp view
checks.write.mode("overwrite").saveAsTable("checks")

# COMMAND ----------

# MAGIC %md ##More wrangling (categories this time)
# MAGIC 
# MAGIC Remember, part of our question is whether or not elite users are more indicative of the crowd when broken down by categories as well.
# MAGIC 
# MAGIC Below the command explodes the list from the categories column into seperate rows for each category. Essentially it will multiply the review by the number of categories it is in and the category column will only have a single category in it.

# COMMAND ----------

checks1 = checks.select("metro_area","review_stars","Is_elite","review_id","business_stars","elite_num_of_reviews","non_elite_num_of_reviews",f.explode(f.split(checks.categories, "\s*,\s*")).alias("category") )
checks1.cache()
checks1.createOrReplaceTempView("checksone")#create temp view
checks1.write.mode("overwrite").saveAsTable("checksone")

# COMMAND ----------

checks1.show()

# COMMAND ----------

# MAGIC %md ## Bringing in the categories.json

# COMMAND ----------

# MAGIC %md We need this .json for defining the top-level categories.

# COMMAND ----------

df_categories = spark.read.option("multiline","true").json("/yelp/categories.json")
print( "number of categories:", df_categories.count() )
df_categories.show()
df_categories.printSchema()

# COMMAND ----------

# MAGIC %md There are 22 top level categories which can be seen below. Some of the top level categories are Europe specific and since we have no metro area outside of North America, not all of these will be relevant to us. Regardless, we will bring all 22 of them in. 

# COMMAND ----------

topcats = df_categories.filter(f.size(df_categories.parents) == 0).select("title").cache()
topcats.show(22,truncate=False)
topcats.createOrReplaceTempView("topcats")#create temp view

# COMMAND ----------

# MAGIC %md One more join. This filters out only reviews in the top level categories dataframe. This will allow us to do our analysis by categories as well.

# COMMAND ----------

df_buscats = spark.sql("""
SELECT B.*
FROM checksone AS B INNER JOIN topcats AS C
ON B.category = C.title
""")
df_buscats.createOrReplaceTempView("reviewcats")

# COMMAND ----------

# MAGIC %md #Analysis 

# COMMAND ----------

# MAGIC %md The next two queries get pretty complicated. All of that wrangling above will finally come into fruition. 
# MAGIC 
# MAGIC There are two queries below, the difference between them is simply the first one is for non-elite users and the second one is for elite users.
# MAGIC 
# MAGIC These queries takes and display the variance, standard deviation, and review count for elite and non-elite reviews. 
# MAGIC 
# MAGIC Earlier we discusssed a problem regarding certain businesses have such a low number of reviews that their mean star rating would be inaccurate. In these queries below I set a somewhat arbitrary cutoff. Any business that has fewer than 20 elite reviews and 20 non-elite reviews will filtered out of the results.

# COMMAND ----------

nonelitemse=spark.sql("""
SELECT (SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id)) AS non_elite_variance, sqrt((SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id))) AS non_elite_standard_deviation,COUNT(C.review_id)
FROM checks AS C
WHERE Is_elite[0] IS NULL AND C.elite_num_of_reviews >= 20 AND C.non_elite_num_of_reviews >= 20
""")

# COMMAND ----------

elitemse=spark.sql("""
SELECT (SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id)) AS elite_variance, sqrt((SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id))) AS elite_standard_deviation,COUNT(C.review_id)
FROM checks AS C
WHERE Is_elite[0] IS NOT NULL AND C.elite_num_of_reviews >= 20 AND C.non_elite_num_of_reviews >= 20
""")

# COMMAND ----------

# MAGIC %md ##Finally, some results
# MAGIC 
# MAGIC Below, we can see that the variance and standard deviation for elite users review ratings is significantly lower than non-elite users. The lower a standard deviation is, the closer the values are to the mean. Looking at this one can be confident in assuming that elite users are more indicative of the crowd than non-elite users. If you were to randomly pick a single review to determine the star rating of a business, choosing an elite review is more often than not going to be closer to the mean and therefore give a better idea of the quality of the business. With the data we have here Yelp can be confident in whatever proprietary algorithm they use to give a user elite-status. 

# COMMAND ----------

nonelitemse.show(truncate= False)
elitemse.show(truncate= False)

# COMMAND ----------

# MAGIC %md ##By metro area
# MAGIC 
# MAGIC Below we have two nearly identifical querys to the ones above. 
# MAGIC 
# MAGIC These queries will return variance, standard deviation, and review count for each metro area.
# MAGIC 
# MAGIC The only difference here is that these are grouped by metro areas instead of taking a look at the whole picture.

# COMMAND ----------

nonelitemse_metro=spark.sql("""
SELECT (SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id)) AS non_elite_variance, sqrt((SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id))) AS non_elite_standard_deviation,COUNT(C.review_id) AS review_count,C.metro_area
FROM checks AS C
WHERE Is_elite[0] IS NULL AND C.elite_num_of_reviews >= 20 AND C.non_elite_num_of_reviews >= 20 
GROUP BY C.metro_area
""")
elitemse_metro=spark.sql("""
SELECT (SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id)) AS elite_variance, sqrt((SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id))) AS elite_standard_deviation,COUNT(C.review_id) AS review_count,C.metro_area
FROM checks AS C
WHERE Is_elite[0] IS NOT NULL AND C.elite_num_of_reviews >= 20 AND C.non_elite_num_of_reviews >= 20 
GROUP BY C.metro_area
""")
nonelitemse_metro.cache()
elitemse_metro.cache()
nonelitemse_metro.show(truncate= False)
elitemse_metro.show(truncate= False)
nonelitemse_metro.write.mode("overwrite").saveAsTable("msereg")
elitemse_metro.write.mode("overwrite").saveAsTable("elitemse")

# COMMAND ----------

# MAGIC %md
# MAGIC | non_elite_variance | elite_variance     | variance difference |  | non_elite_sd | elite_sd    | sd difference |  | review_count | elite review_count | metro_area |
# MAGIC |--------------------|--------------------|---------------------|--|--------------|-------------|---------------|--|--------------|--------------------|------------|
# MAGIC | 1.666111744        | 0.905678771        | 0.760432974         |  | 1.29077951   | 0.951671567 | 0.339107944   |  | 739680       | 150162             | AZ         |
# MAGIC | 1.279266316        | 0.75279491         | 0.526471406         |  | 1.131046558  | 0.867637545 | 0.263409012   |  | 53677        | 26845              | QC         |
# MAGIC | 1.715791407        | 0.954919092        | 0.760872315         |  | 1.309882211  | 0.977199617 | 0.332682594   |  | 1174186      | 295370             | NV         |
# MAGIC | 1.473114592        | 0.766569969        | 0.706544623         |  | 1.213719322  | 0.875539816 | 0.338179506   |  | 46632        | 16858              | WI         |
# MAGIC | 1.603081241        | 0.812371645        | 0.790709596         |  | 1.266128446  | 0.901316618 | 0.364811828   |  | 146518       | 48995              | NC         |
# MAGIC | 1.416873044        | 0.968581542        | 0.448291502         |  | 1.190324764  | 0.984165404 | 0.20615936    |  | 9053         | 1734               | IL         |
# MAGIC | 1.584531565        | 0.838337419        | 0.746194146         |  | 1.258781778  | 0.915607678 | 0.343174101   |  | 90276        | 33021              | OH         |
# MAGIC | 1.535282126        | 0.801610825        | 0.733671301         |  | 1.239065021  | 0.895327217 | 0.343737804   |  | 105659       | 41100              | PA         |
# MAGIC | 1.501976392        | 0.852990087        | 0.648986305         |  | 1.225551464  | 0.923574625 | 0.30197684    |  | 257496       | 138334             | ON         |
# MAGIC | 1.434480016        | 0.771153259        | 0.663326757         |  | 1.197697798  | 0.878153323 | 0.319544475   |  | 14792        | 6843               | AB         |

# COMMAND ----------

displayHTML( showimage("/dbfs/FileStore/tables/metro_Sd-4e924.png",100) )


# COMMAND ----------

displayHTML( showimage("/dbfs/FileStore/tables/metro_variance-1ed39.png",100) )

# COMMAND ----------

# MAGIC %md Above we can see that our standard deviation and variance is lower for elite users in every metro area sans Florida. There was not supposed to be any Flordia businesses in our data set, this is the product of unclean data. There are actually erroneous reviews from Texas as well. The thing is, in the entire state of Florida there are less than 1000 reviews and of elite reviews there are only 64. We are going to ignore the Florida and Texas data because it wasn't supposed to be there in the first place and the sample size is to small to draw any conclusions from. 
# MAGIC 
# MAGIC If we ignore Florida and Texas all of the elite standard deviations and variances of review star ratings are below 1.0 and all non-elite standard deviations and variances are above 1.0.
# MAGIC 
# MAGIC #####We can conclude that elite users are more indicative of the crowd when broken down by geographical area as well.

# COMMAND ----------

# MAGIC %md ##Breakdown by Categories
# MAGIC 
# MAGIC These same queries again, just like the ones above but instead they are grouped by categories instead of metro area.

# COMMAND ----------

nonelitemse_cat=spark.sql("""
SELECT (SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id)) AS non_elite_variance, sqrt((SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id))) AS non_elite_standard_deviation,COUNT(C.review_id) AS review_count,C.category
FROM reviewcats AS C
WHERE Is_elite[0] IS NULL AND C.elite_num_of_reviews >= 20 AND C.non_elite_num_of_reviews >= 20 
GROUP BY C.category
""")
elitemse_cat=spark.sql("""
SELECT (SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id)) AS elite_variance, sqrt((SUM((C.business_stars - C.review_stars)*(C.business_stars - C.review_stars)))/(COUNT(C.review_id))) AS elite_standard_deviation,COUNT(C.review_id) AS review_count,C.category
FROM reviewcats AS C
WHERE Is_elite[0] IS NOT NULL AND C.elite_num_of_reviews >= 20 AND C.non_elite_num_of_reviews >= 20 
GROUP BY C.category
""")



# COMMAND ----------

nonelitemse_cat.show(truncate= False)


# COMMAND ----------

elitemse_cat.show(truncate= False)

# COMMAND ----------

# MAGIC %md
# MAGIC | non_elite_variance | elite_variance     | variance difference |  | non_elite_sd | elite_sd    | sd difference |  | non-elite review_count | elite review_count | category                     |
# MAGIC |--------------------|--------------------|---------------------|--|--------------|-------------|---------------|--|------------------------|--------------------|------------------------------|
# MAGIC | 1.969418295        | 0.986869626        | 0.982548668         |  | 1.403359646  | 0.99341312  | 0.409946526   |  | 48401                  | 11712              | Beauty & Spas                |
# MAGIC | 1.634054983        | 0.806402302        | 0.827652681         |  | 1.278301601  | 0.897999055 | 0.380302546   |  | 5571                   | 2412               | Education                    |
# MAGIC | 1.526530659        | 0.828392658        | 0.698138001         |  | 1.235528494  | 0.910160787 | 0.325367707   |  | 732642                 | 228199             | Food                         |
# MAGIC | 1.471644515        | 0.442429223        | 1.029215292         |  | 1.213113562  | 0.665153534 | 0.547960029   |  | 50                     | 21                 | Financial Services           |
# MAGIC | 1.522626252        | 0.752710017        | 0.769916236         |  | 1.233947427  | 0.867588622 | 0.366358805   |  | 5205                   | 3584               | Public Services & Government |
# MAGIC | 1.716641589        | 0.908767219        | 0.80787437          |  | 1.310206697  | 0.95329283  | 0.356913867   |  | 703669                 | 203721             | Nightlife                    |
# MAGIC | 1.862557611        | 0.964641824        | 0.897915788         |  | 1.364755513  | 0.982161811 | 0.382593702   |  | 237299                 | 83200              | Arts & Entertainment         |
# MAGIC | 1.817036752        | 0.847553126        | 0.969483626         |  | 1.347975056  | 0.920626485 | 0.427348571   |  | 60838                  | 21918              | Active Life                  |
# MAGIC | 1.828666525        | 0.978512067        | 0.850154458         |  | 1.352281969  | 0.989197688 | 0.363084281   |  | 285633                 | 79638              | Event Planning & Services    |
# MAGIC | 1.912500664        | 0.982653151        | 0.929847513         |  | 1.382931909  | 0.991288632 | 0.391643277   |  | 7292                   | 2232               | Professional Services        |
# MAGIC | 2.083131789        | 0.811281925        | 1.271849864         |  | 1.443305854  | 0.900711899 | 0.542593955   |  | 3914                   | 939                | Pets                         |
# MAGIC | 1.087326263        | 0.492415341        | 0.594910923         |  | 1.042749377  | 0.701723122 | 0.341026255   |  | 851                    | 569                | Religious Organizations      |
# MAGIC | 2.217994085        | 1.28140702         | 0.936587065         |  | 1.489293149  | 1.1319925   | 0.357300649   |  | 7620                   | 1516               | Health & Medical             |
# MAGIC | 1.878483922        | 0.885915926        | 0.992567996         |  | 1.370577952  | 0.941231069 | 0.429346883   |  | 74181                  | 32321              | Shopping                     |
# MAGIC | 2.20402259         | 1.187528358        | 1.016494232         |  | 1.484595093  | 1.089737747 | 0.394857346   |  | 24007                  | 4865               | Automotive                   |
# MAGIC | 1.529030157        | 0.801856177        | 0.72717398          |  | 1.23653959   | 0.895464225 | 0.341075365   |  | 51127                  | 19349              | Local Flavor                 |
# MAGIC | 1.580344984        | 0.93116691         | 0.649178074         |  | 1.257117729  | 0.964969901 | 0.292147828   |  | 13759                  | 2746               | Home Services                |
# MAGIC | 1.625731576        | 0.888467025        | 0.737264551         |  | 1.275041794  | 0.942585288 | 0.332456506   |  | 2231574                | 600443             | Restaurants                  |
# MAGIC | 1.982060621        | 1.084116933        | 0.897943688         |  | 1.407856747  | 1.041209361 | 0.366647387   |  | 178198                 | 55342              | Hotels & Travel              |
# MAGIC | 1.98503724         | 0.824348169        | 1.160689071         |  | 1.408913496  | 0.907936214 | 0.500977282   |  | 607                    | 274                | Mass Media                   |

# COMMAND ----------

# MAGIC %md We can see that financial services, pets, religious organizations, and mass media categories should all be ignored due to such low review counts.

# COMMAND ----------


displayHTML( showimage("/dbfs/FileStore/tables/CATVariance.png",100) )

# COMMAND ----------

displayHTML( showimage("/dbfs/FileStore/tables/CATSD.png",100) )

# COMMAND ----------

# MAGIC %md # Final Results 
# MAGIC 
# MAGIC Nothing has changed. Elite users have lower standard deviations and variances of star ratings across the board. 
# MAGIC #####This means that elite user reviews are much closer to the business star mean than non-elite user reviews.
# MAGIC 
# MAGIC It doesn't matter the metro area and it doesn't matter the category chosen, elite reviews are just better.
# MAGIC 
# MAGIC If you were to pick any review at random to judge what the star rating of a business should be(and therefor the quality fo the business) you are far better off using an elite user's review rather than a non-elite user's review. 
# MAGIC 
# MAGIC Elite users have a tighter distribution around the mean than non-elite users and elite users are alse less prone to making extreme reviews. 
# MAGIC 
# MAGIC #####Elite users do a good job of accurately reviewing businesses and Yelp would be correct to keep their elite user decision algorythm intact.
