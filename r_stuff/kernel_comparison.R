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
  return (sort(discrepancyCount))
}

legendVector = c("2012 Version: 2.6.32-279.el6.x86_64", "2013 Version: 2.6.32-358.el6.x86_64", "2014 Version: 2.6.32-431.17.1.el6.x86_64")
legendColors = c("#d7191c", "darkorchid", "deepskyblue4")

kern2012 = dbqueryFN(1329870746, 1371903561)
kern2013 = dbqueryFN(1371903561, 1399505569)
kern2014 = dbqueryFN(1399505569, 1470331649)
label = c(names(kern2012), names(kern2013))

plot(kern2012,
     type="l",
     col = "#d7191c",
     main="Discrepancies Per Host Based on 2012-2014 Kernels",
     xlab="HostID",
     ylim=c(1, max(kern2013)),
     ylab="Frequency",
     xaxt="n")
axis(1, at=1:length(label), labels=label)
lines(kern2013, type="l", col="darkorchid")
lines(kern2014, type="l", col="deepskyblue4")
legend(1, max(kern2013), legend=legendVector, col=legendColors, lty=1, title="Kernel Legend", bg="darkgray")