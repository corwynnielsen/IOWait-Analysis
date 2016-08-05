  library(RMySQL)
  library(stringr)
  library(gplots)
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
    colNames = vector()
    rowNames = vector()
    for(hostid in dat[['hosts']]){
      hostQuery = dbSendQuery(dbcon,  paste("SELECT hostname as host FROM hosts WHERE id = " , hostid, " ;"))
      hostDat = fetch(hostQuery, n = -1)
      shortenedHostname = str_match(hostDat[['host']], "c\\d\\d\\d-\\d\\d\\d")
      unusedHostnames = c("105", "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116")
      validCol = !is.element(substr(shortenedHostname, 1, 4), colNames) && substr(shortenedHostname, 1, 4) != "c400"
      if (validCol){
        colNames = c(colNames, substr(shortenedHostname, 1, 4))
      }
      validRow = !is.element(substr(shortenedHostname, 6, 8), rowNames) && !is.element(substr(shortenedHostname, 6, 8), unusedHostnames)
      if (validRow){
        rowNames = c(rowNames, substr(shortenedHostname, 6, 8))
      }
      hostVector = c(hostVector, shortenedHostname)
    }
    
    dbDisconnect(dbcon)
    
    names(discrepancyCount) = hostVector
    
    m = data.frame(matrix(0, ncol=length(colNames), nrow=length(rowNames)))
    names(m) = sort(colNames)
    row.names(m) = sort(rowNames)
    return (populateDF(m, discrepancyCount, unusedHostnames))
  }
  
  populateDF = function(frame, data, invalidData){
    index = 1
    for (i in 1:length(data)){
      row = substr(names(data[index]), 6, 8)
      col = substr(names(data[index]), 1, 4)
      name = names(data[index])
      if (!is.element(row, invalidData) && col != "c400"){
        if (is.element(row, substr(name, 6, 8))){
          frame[row, col] = data[name]
        }
      }
      index = index + 1
    }
    return(frame)
  }
  myPalette = colorRampPalette(c("forestgreen", "yellow", "orange", "red"))(n=150)
  heatmap.2(as.matrix(dbqueryFN(0,1483228800)), 
            dendrogram = "none", 
            trace = "none", 
            density.info = "histogram", 
            denscol="black",
            Rowv=F, 
            Colv=F, 
            col=myPalette,
            margins=c(5,0),
            key.par=list(mar=c(3.5,0,3,0)),
            lmat=rbind(c(5,4,2),c(3,1,6)), lhei=c(1.4,5), lwid=c(1,7,1)
            )