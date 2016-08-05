  library(RMySQL)
  library(stringr)
  library(getPass)
  
  pass = getPass::getPass(msg="Enter SQL database password for xdtas")
  
  con =  dbConnect(MySQL(), host='128.205.11.48', port=3306, user='xdtas', pass=pass, dbname='ts_analysis')
  discrepencyCountQuery = dbSendQuery(con, 'SELECT hostid as hosts,  count(*) AS num FROM dataerrors WHERE discrepency > 10 GROUP BY hostid;')
  dat <- fetch(discrepencyCountQuery, n = -1)
  discrepancyNumbers = dat[['num']]
  
  hostVector = vector()
  for(hostid in dat[['hosts']]){
    hostQuery = dbSendQuery(con,  paste("SELECT hostname as host FROM hosts WHERE id = " , hostid, " ;"))
    hostDat = fetch(hostQuery, n = -1)
    short = str_match(hostDat[['host']], "c\\d\\d\\d-\\d\\d\\d")
    hostVector =  c(hostVector,  short)
  }
  
  dbDisconnect(con)
  
  
  barplot(discrepancyNumbers,
          col = "darkslategray1",
          names.arg = hostVector,
          las = 2,
          main =  "Amount of Discrepancies Over 10 per Host",
          ylab = "Frequency"
  )
