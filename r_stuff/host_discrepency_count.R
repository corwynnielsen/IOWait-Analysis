library(RMySQL)
library(stringr)
library(getPass)

pass = getPass::getPass(msg="Enter SQL database password for xdtas")

con <- dbConnect(MySQL(), host='128.205.11.48', port=3306, user='xdtas', pass=pass, dbname='ts_analysis')
rs <- dbSendQuery(con, ' SELECT hostid as hosts,  count(*) AS num FROM dataerrors GROUP BY hostid;')
dat <- fetch(rs,n = -1)
discrepancyNumbers = dat[['num']]

hostVector = vector()
for(hostid in dat[['hosts']]){
  hostQuery = dbSendQuery(con, paste("SELECT hostname as host FROM hosts WHERE id = " , hostid, " ;"))
  hostDat = fetch(hostQuery, n = -1)
  short = str_match(hostDat[['host']], "c\\d\\d\\d-\\d\\d\\d")
  hostVector =  c(hostVector,  short)
}

names(discrepancyNumbers) = hostVector
dbDisconnect(con)

barplot(discrepancyNumbers,
        col = "aquamarine4",
        names.arg = hostVector,
        las = 2,
        main =  "Amount of Discrepancies per Host",
        ylab = "Frequency"
        )
