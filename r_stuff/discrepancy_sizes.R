library(RMySQL)
library(getPass)

pass = getPass::getPass(msg="Enter SQL database password for xdtas")

con <- dbConnect(MySQL(), host='128.205.11.48', port=3306, user='xdtas', pass=pass, dbname='ts_analysis')

rs <- dbSendQuery(con, statement = 'SELECT discrepency as d from dataerrors where discrepency < 50' )
dat <- fetch(rs, n = -1)

discrepancies = dat[['d']]
dbDisconnect(con)
hist(discrepancies,  
      col=colorRampPalette(c("blue", "red"))(20) , 
      main="IOWait Discrepency Analysis",
      xlab="Error Size",
      ylab="Error Frequency",
      labels=T,
      right=F,
      breaks=50
      )
abline(v=mean(discrepancies), col="cyan")
print(mean(discrepancies))