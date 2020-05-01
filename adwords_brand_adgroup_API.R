library("RAdwords")
library(httr)
library(plyr)
library(dplyr)
library(reticulate)
library(DBI)
load("/home/ubuntu/Desktop/r_yuvansh/auth_file.RData")

##click, cost ,impression,
##CTR , session, evaluation, appointment , verified appointment , purchase , inspection
body2 <- statement(select = c('Date','AdGroupId','AdGroupName', 'CampaignId','CampaignName', 'Impressions', 'Clicks', 'Cost'),
                   report = "ADGROUP_PERFORMANCE_REPORT",
                   start = Sys.Date()-1,
                   end = Sys.Date()-1)

#start = '2019-07-12',
#end = Sys.Date()-1)

data2 <- getData(clientCustomerId = '565-781-9352',
                 google_auth = google_auth,
                 statement = body2)


colnames(data2)<-c("NEW_DATE","CAMPAIGN_ID","CAMPAIGN","AD_GROUP_ID","AD_GROUP","IMPRESSIONS","CLICKS","COST")
source_python("/home/ubuntu/Desktop/r_yuvansh/nntp.py")
insert_to_snowflake <- ads_googly(data2,"adwords_brand_adgroup")
