library(RMySQL)
library(stringr)
library(getPass)

pass = getPass::getPass(msg="Enter SQL database password for xdtas")

dbqueryFN = function(startTS, endTS){
  dbcon = dbConnect(MySQL(), host='128.205.11.48', port=3306, user='xdtas', pass=pass, dbname='ts_analysis')
  
  discrepancyCountQuery = dbSendQuery(dbcon, paste("SELECT 
                                                        hostid AS hosts, COUNT(*) AS num
                                                    FROM
                                                        dataerrors
                                                    WHERE
                                                        timestamp > ", startTS, "
                                                            AND timestamp < ",endTS, "
                                                    GROUP BY hostid;"))
  dat = fetch(discrepancyCountQuery, n = -1)
  
  discrepancyCount = dat[['num']]
  
  hostVector = vector()
  for(hostid in dat[['hosts']]){
    hostQuery = dbSendQuery(dbcon,  paste("SELECT 
                                                hostname AS host
                                            FROM
                                                hosts
                                            WHERE
                                                id = " , hostid, ";"))
    hostDat = fetch(hostQuery, n = -1)
    shortenedHostname = str_match(hostDat[['host']], "c\\d\\d\\d-\\d\\d\\d")
    hostVector =  c(hostVector,  shortenedHostname)
  }
  
  dbDisconnect(dbcon)
  
  names(discrepancyCount) = dat[['hosts']]
  return (discrepancyCount)
}


legendVector = c("2013", "2014", "2015", "2016")
legendColors = c("#d7191c", "#fdae61", "#abd9e9", "#2c7bb6")

dat2013 = dbqueryFN(1356998400, 1388534400)
dat2014 = dbqueryFN(1388534400, 1420070400)
dat2015 = dbqueryFN(1420070400, 1451606400)
dat2016 = dbqueryFN(1451606400, 1483228800)

plot(dat2013,
     type="l",
     col="#d7191c",
     main="Discrepancies Per Host: 2013-present",
     xlab="HostID",
     ylab="Frequency",
     xaxt="n")
axis(1,at=1:length(dat2013), labels=names(dat2013))
lines(dat2014, type="l", col = "#fdae61")
lines(dat2015, type="l", col = "#abd9e9")
lines(dat2016, type="l", col = "#2c7bb6")
legend(1, max(dat2013), legend=legendVector, col=legendColors, lty=1, title="Year Legend", bg="darkgray")
