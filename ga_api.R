#date_min<-as.Date(min(channel$timestamp))-1
#date_max<-as.Date(max(channel$timestamp))+1
#min<-seq(0,59,by=1)
#hour<-seq(0,23,by=1)
library(bsts)
library(httr)
require(RGoogleAnalytics)
library(ggplot2)
library(googleAuthR)
library(plyr)
library(dplyr)
library(googleAnalyticsR)
library(data.table)
library(stringr)
library(sqldf)
ga_auth()
account_list <- ga_account_list()
client_id<-"972396386986-u57er0h96p3m9h3j7urre3c3cjqp5stq.apps.googleusercontent.com"
secret<-"fojbeshq9AhSmKQqNae2AvA3"
token <- Auth(client_id,secret)
save(token,file="./token_file")
load("C:/Users/User/Documents/token_file")
ValidateToken(token)
ga_id <- 106177942
#mf3 <- met_filter("goal4Completions","EQUAL","0",not = TRUE)
#fc2 <- filter_clause_ga4(list(mf3), operator = "AND")
#emailId: 1
#sessionid: 2
#clientid: 4
#appt_id : 5
#df1 <- dim_filter("sourceMedium","REGEXP","organic",not = FALSE)
#df2 <- dim_filter("sourceMedium","REGEXP","direct",not = FALSE)
#df3 <- dim_filter("campaign","REGEXP","brand",not = FALSE)
#df4 <- dim_filter("sourceMedium","REGEXP","cars24_android / (not set)",not=FALSE)
#df5 <- dim_filter("country","EXACT","India",not = FALSE)
#fc2 <- filter_clause_ga4(list(df1,df2,df3,df4,df5), operator = "OR")
#g<-seq(date_min,date_max,by='days')
g<-seq(as.Date('2019-01-01'),as.Date('2019-01-24'),by='days')
data<-as.data.frame(expand.grid(g,hour,min))
data<-data[order(data[,1],data[,2],data[,3]),]
colnames(data)<-c("date","hour","minute")
#data$date<-gsub("-","",data$date)
data$hour<- str_pad(data$hour, width=2, side="left", pad="0")
data$minute<- str_pad(data$minute, width=2, side="left", pad="0")

ga_data3<-as.data.frame(matrix(ncol=6,nrow=0))
colnames(ga_data3)<-c("date","city","country","hour","minute","sessions")
h<-length(g)
for(i in 1:h)
{
  j<-g[i]
  
  query.list <- Init(start.date = as.character(j),
                     end.date = as.character(j),
                     dimensions = "ga:date,ga:city,ga:country,ga:hour,ga:minute",
                     metrics = "ga:sessions",
                     max.results = 1000001,
                     #sort = "-ga:transactions",
                     table.id = "ga:106177942")
  
  ga.query <- QueryBuilder(query.list)
  temp <- GetReportData(ga.query,token,paginate_query = T)
  ga_data3<-rbind(ga_data3,temp)
  
}
ga_data4<-ga_data3[which(ga_data3$country  == "India"),]
t<-read.csv("C:/Users/User/Desktop/reg_city.csv")
t$City<-as.character(trimws(t$City))
ga_data4$city<-as.character(trimws(ga_data4$city))

t1<-merge(ga_data4,t,by.x="city",by.y="City",all.x = T)
t2<-t1[which(t1$Region %in% c('NDL','WGJ','NRJ','MUM','PUN','SKA','NUP','STN','STS')),]
ga_data5<-t2[,c(2,4,5,6,7)]
ga_data5$date<-as.Date(ga_data5$date,"%Y%m%d")
ga_data5$hour<- str_pad(ga_data5$hour, width=2, side="left", pad="0")
ga_data5$minute<- str_pad(ga_data5$minute, width=2, side="left", pad="0")
ndl<-ga_data5[which(ga_data5$Region=="NDL"),]
wgj<-ga_data5[which(ga_data5$Region=="WGJ"),]
nrj<-ga_data5[which(ga_data5$Region=="NRJ"),]
mum<-ga_data5[which(ga_data5$Region=="MUM"),]
pun<-ga_data5[which(ga_data5$Region=="PUN"),]
ska<-ga_data5[which(ga_data5$Region=="SKA"),]
nup<-ga_data5[which(ga_data5$Region=="NUP"),]
stn<-ga_data5[which(ga_data5$Region=="STN"),]
sts<-ga_data5[which(ga_data5$Region=="STS"),]
options(sqldf.driver = "SQLite")
total_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from ga_data5 group by 1,2,3")
#total_s<-ddply(ga_data5,.(date,hour,minute),summarise,
#             sessions=sum(sessions))
colnames(total_s)<-c("date","hour","minute","total_sessions")
ndl_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from ndl group by 1,2,3")
# ndl_s<-ddply(ndl,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(ndl_s)<-c("date","hour","minute","ndl_sessions")
wgj_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from wgj group by 1,2,3")
# wgj_s<-ddply(wgj,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(wgj_s)<-c("date","hour","minute","wgj_sessions")
nrj_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from nrj group by 1,2,3")
# nrj_s<-ddply(nrj,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(nrj_s)<-c("date","hour","minute","nrj_sessions")
mum_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from mum group by 1,2,3")
# mum_s<-ddply(mum,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(mum_s)<-c("date","hour","minute","mum_sessions")
pun_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from pun group by 1,2,3")
# pun_s<-ddply(pun,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(pun_s)<-c("date","hour","minute","pun_sessions")
ska_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from ska group by 1,2,3")
# ska_s<-ddply(ska,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(ska_s)<-c("date","hour","minute","ska_sessions")
nup_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from nup group by 1,2,3")
# nup_s<-ddply(nup,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(nup_s)<-c("date","hour","minute","nup_sessions")
stn_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from stn group by 1,2,3")
# stn_s<-ddply(stn,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(stn_s)<-c("date","hour","minute","stn_sessions")
sts_s<-sqldf("select date,hour,minute,sum(sessions) as sessions from sts group by 1,2,3")
# sts_s<-ddply(sts,.(date,hour,minute),summarise,
#              sessions=sum(sessions))
colnames(sts_s)<-c("date","hour","minute","sts_sessions")

data0<-merge(data,total_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data1<-merge(data0,wgj_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data2<-merge(data1,nrj_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data3<-merge(data2,ska_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data4<-merge(data3,mum_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data5<-merge(data4,pun_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data6<-merge(data5,nup_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data7<-merge(data6,stn_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data8<-merge(data7,sts_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data9<-merge(data8,ndl_s,by.x = c("date","hour","minute"),by.y = c("date","hour","minute"),all.x = T)
data9[is.na(data9)]<-0
write.csv(data9,"C:/Users/User/Desktop/ga_sessions_january.csv",row.names = F)
  