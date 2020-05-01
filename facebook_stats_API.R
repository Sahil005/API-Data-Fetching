library(rfacebookstat)
library(lubridate)
library(RMySQL)
# Auth
account="act_773517116104203"
token <- "EAAC9M3ohq60BABev9kzUuP49NkU74wUbXVhFbHabnCZCPrm4oSmS1gSPCcUXd5ZASv57YqCO2oZBrhNqT3dxreBu9ZCj6if9LEFFJmSPyOKZCUFaQc5IBG45fXYWM1TmNxZAm3DWFi5fl1kzK7ZCAQcBmFACZB1b0MmZBOZBqKZB4oGZBE3588UzKZAcqnJ5ZB0UcGWHIZD"
# Get statistic
account_level2 <- fbGetMarketingStat(accounts_id = account,
                                level = "ad", fields = "campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,reach,clicks,ctr,spend",
                                interval = "day",breakdowns = "region",request_speed = "fast",date_start = "2019-01-25", date_stop = Sys.Date()-1,api_version = "v3.2",
                                access_token = token)

con1 <- dbConnect(MySQL(),
                  user="Vijendra", password="Vijju",
                  dbname="BI", host="13.126.177.49")

dbWriteTable(con1,"facebook",account_level2,append=T,row.names=F)



