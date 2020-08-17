####RUN THURSDAY

library(DBI)
con <- dbConnect(odbc::odbc(), "Azure_test", uid = "sacondiscuser", 
                 pwd = "Ax#4qiZV#PQR", timeout = 10)

checknetwork<-dbGetQuery(con, "select * from sctemp.Ob_Network_actuals")

checkus13<-dbGetQuery(con, "select * from sctemp.Ob_us13_actuals")

checkus16<-dbGetQuery(con, "select * from sctemp.Ob_us16_actuals")

checkus19<-dbGetQuery(con, "select * from sctemp.Ob_us19_actuals")




data_13<-checkus13
data_13$Group.date<-as.Date(data_13$Group.date)
data13<-data_13




data_16<-checkus16
data_16$Group.date<-as.Date(data_16$Group.date)
data16<-data_16




data_19<-checkus19
data_19$Group.date<-as.Date(data_19$Group.date)
data19<-data_19



library(fpp2)
library(dplyr)
library(lubridate)

##13
data13$Group.date<-as.Date(data13$Group.date)
data13 <- data13 %>%
  filter(Group.date >= as.Date('2017-05-01'))

myts <- ts(data13$TOTAL_CASES, start = decimal_date(as.Date("2017-05-01")), frequency = 365)

fcast <- stlf(myts,  h=30,level=80)
dates<-c(Sys.Date() + c(0:29))

Forecasts<-as.data.frame(fcast)
Forecasts$Date<-dates

rownames(Forecasts)<-dates

locked_week13<-Forecasts[12:18,1]

###16

data16$Group.date<-as.Date(data16$Group.date)
data16 <- data16 %>%
  filter(Group.date >= as.Date('2017-05-01'))

myts <- ts(data16$TOTAL_CASES, start = decimal_date(as.Date("2017-05-01")), frequency = 365)

fcast <- stlf(myts,  h=30,level=80)
dates<-c(Sys.Date() + c(0:29))

Forecasts<-as.data.frame(fcast)
Forecasts$Date<-dates

rownames(Forecasts)<-dates

locked_week16<-Forecasts[12:18,1]


###19

data19$Group.date<-as.Date(data19$Group.date)
data19 <- data19 %>%
  filter(Group.date >= as.Date('2017-05-01'))

myts <- ts(data19$TOTAL_CASES, start = decimal_date(as.Date("2017-05-01")), frequency = 365)

fcast <- stlf(myts,  h=30,level=80)
dates<-c(Sys.Date() + c(0:29))

Forecasts<-as.data.frame(fcast)
Forecasts$Date<-dates

rownames(Forecasts)<-dates

locked_week19<-Forecasts[12:18,1]
date<-Forecasts$Date[12:18]

locked_weekNetwork<-locked_week13+locked_week16+locked_week19

Output<-data.frame(locked_week13,locked_week16,locked_week19,locked_weekNetwork,date)


# dbCreateTable(con, SQL("sctemp.Ob_forecast"), Output)
# dbWriteTable(con, SQL("sctemp.Ob_ETS_forecast_dev"), Output)
#dbWriteTable(con, SQL("sctemp.Ob_ETS_forecast_prod"), Output)



#dbAppendTable(con, SQL("sctemp.Ob_forecast"), Output)# dont use 
#dbAppendTable(con, SQL("sctemp.Ob_ETS_forecast_dev"), Output) ## dev
dbAppendTable(con, SQL("sctemp.Ob_ETS_forecast_prod"), Output) ## prod



#dbGetQuery(con, "select * from sctemp.Ob_ETS_forecast_dev") ##dev

#dbGetQuery(con, "select * from sctemp.Ob_forecast") ## dont use 

#dbGetQuery(con, "select * from sctemp.Ob_ETS_forecast_prod") ## prod use this 





#### dont run
#output<-data.frame(sum(locked_week13),sum(locked_week16),sum(locked_week19),sum(locked_weekNetwork))

#setwd("\\Users\\BMafarj1\\OneDrive - JNJ\\OB Forecast\\Results")

#write.csv(output,"JJweek27.csv")

