# batch_job_A
Batch Job A: compute commonly used routes (store and work with pick-up and drop-off pairs)

upon return of each board, save its start and end points as a route
read through all routes for a specified time period
report on routes:
	- for each start station
	- for each end station
	- top 10 routes overall
	- routes for each user


methods
for user-specific jobs:
	- findOne to get user's routes from DB
once routes have been retrieved
 MapReduce function to sort and count routes
 	- this will save the results
	- and then output them to the html served to the page where the graph will be constructed
