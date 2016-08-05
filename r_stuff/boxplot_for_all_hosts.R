library(RMySQL)
library(stringr)
library(getPass)

pass = getPass::getPass(msg="Enter SQL database password for xdtas")

dbqueryFN = function(startTS, endTS){
  dbcon = dbConnect(MySQL(), host='128.205.11.48', port=3306, user='xdtas', pass=pass, dbname='ts_analysis')
  
  discrepancyCountQuery = dbSendQuery(dbcon, paste("SELECT hostid as hosts, count(*) AS num 
                                                  FROM dataerrors 
                                                  WHERE timestamp > ", startTS, " AND timestamp < ",endTS, 
                                                   " GROUP BY hostid;"))
  dat = fetch(discrepancyCountQuery, n = -1)
  
  discrepancyCount = dat[['num']]
  
  hostVector = vector()
  for(hostid in dat[['hosts']]){
    hostQuery = dbSendQuery(dbcon,  paste("SELECT hostname as host FROM hosts WHERE id = " , hostid, " ;"))
    hostDat = fetch(hostQuery, n = -1)
    shortenedHostname = str_match(hostDat[['host']], "c\\d\\d\\d-\\d\\d\\d")
    hostVector =  c(hostVector,  shortenedHostname)
  }
  
  dbDisconnect(dbcon)
  
  names(discrepancyCount) = hostVector
  return (discrepancyCount)
}

a = boxplot(dbqueryFN(0,1483228800),
            show.names=TRUE,
            main="Number of Discrepancies For All Hosts",
            names = c("All hosts"),
            col="darkorchid4",
            horizontal = TRUE)

text(x=a$stats, labels=a$stats, y=1.25)