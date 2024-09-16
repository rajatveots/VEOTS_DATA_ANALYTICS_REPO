We are providing raw data to clients in form of single table. ADMIN_BUSINESS_INSIGHTS_V and ADMIN_TRACKING_INSIGHTS_V are the views 
from which we export the raw data to the client. We use copy into command to store the data into S3 bucket. From S3 bucket the data 
will be available on the web app.

We have created a lookup table to specify what client and which month data should be loaded. Using that lookup table, we use cursor 
to loop over the lookup table and load the copy into statement for each record in lookup table.