##RUN THURSDAY input before output

#dbWriteTable(con, SQL("sctemp.Ob_Network_actuals"), data_full)
#dbWriteTable(con, SQL("sctemp.Ob_us13_actuals"), data_13)
#dbWriteTable(con, SQL("sctemp.Ob_us16_actuals"), data_16)
#dbWriteTable(con, SQL("sctemp.Ob_us19_actuals"), data_19)





library(DBI)
con <- dbConnect(odbc::odbc(), "RP_SDDC_Archive", uid = "BMafarj1", 
                 pwd = "Mafarjeh20#", timeout = 10)

data_new<-dbGetQuery(con, "select Trunc(dt.TRNDTE) trndte,
	dt.WH_ID,
	dt.PRTNUM,
	o.STCUST,
	o.ORDTYP,
	Sum(dt.TRNQTY) total_cases,
	Sum(Decode(pw.LODLVL, 'L', 0, dt.TRNQTY)) loose_cases,
	Count(Distinct Decode(pw.LODLVL, 'L', dt.LODNUM, Null)) full_pallets,
	o.ORDNUM 
from 
	(
	select Min(arcwmp.dlytrn.TRNDTE) trndte,
		arcwmp.dlytrn.WH_ID,
		arcwmp.dlytrn.LODNUM,
		Sum(arcwmp.dlytrn.TRNQTY) trnqty,
		arcwmp.dlytrn.PRTNUM 
	from arcwmp.dlytrn 
	where arcwmp.dlytrn.ACTCOD = 'SHIPLOAD' 
		and arcwmp.dlytrn.TRNDTE Between TO_DATE(TO_CHAR(SYSDATE-8, 'yyyy-mon-dd'), 'yyyy-mon-dd')
		and TO_DATE(TO_CHAR(SYSDATE-1, 'yyyy-mon-dd'), 'yyyy-mon-dd')
	group by arcwmp.dlytrn.WH_ID, arcwmp.dlytrn.LODNUM, arcwmp.dlytrn.PRTNUM, Trunc(arcwmp.dlytrn.TRNDTE)
	) dt 
	left outer join 
	(
	select w.WH_ID,
		s.LODNUM,
		d.PRTNUM,
		Min(w.ORDNUM) ordnum,
		Min(w.CLIENT_ID) client_id,
		Max(w.LODLVL) lodlvl 
	from arcwmp.invsub s 
		inner join arcwmp.invdtl d on d.SUBNUM = s.SUBNUM 
		inner join 
		(
		select arcwmp.pckwrk.WRKREF,
			arcwmp.pckwrk.SHIP_LINE_ID,
			arcwmp.pckwrk.WH_ID,
			arcwmp.pckwrk.LODLVL,
			arcwmp.pckwrk.SHIP_ID,
			arcwmp.pckwrk.ORDNUM,
			arcwmp.pckwrk.CLIENT_ID 
		from arcwmp.pckwrk 
		where arcwmp.pckwrk.PCKDTE Between TO_DATE(TO_CHAR(SYSDATE-8, 'yyyy-mon-dd'), 'yyyy-mon-dd') - 29 
			and TO_DATE(TO_CHAR(SYSDATE-1, 'yyyy-mon-dd'), 'yyyy-mon-dd')
		) w on w.WRKREF = d.WRKREF and w.SHIP_LINE_ID = d.SHIP_LINE_ID 
	group by w.WH_ID, s.LODNUM, d.PRTNUM
	) pw on pw.LODNUM = dt.LODNUM and pw.WH_ID = dt.WH_ID and pw.PRTNUM = dt.PRTNUM 
	left outer join arcwmp.ord o on o.ORDNUM = pw.ORDNUM and o.CLIENT_ID = pw.CLIENT_ID and o.WH_ID = pw.WH_ID and dt.TRNDTE Between TO_DATE(TO_CHAR(SYSDATE-8, 'yyyy-mon-dd'), 'yyyy-mon-dd') and TO_DATE(TO_CHAR(SYSDATE-1, 'yyyy-mon-dd'), 'yyyy-mon-dd') 
group by Trunc(dt.TRNDTE), dt.WH_ID, dt.PRTNUM, o.STCUST, o.ORDTYP, o.ORDNUM 
order by dt.WH_ID, trndte, dt.PRTNUM, o.STCUST, o.ORDTYP")


#TO_DATE(TO_CHAR(SYSDATE, 'dd-mon-yyyy'), 'dd-mon-yyyy')

data_new$TRNDTE<-as.Date(data_new$TRNDTE)

data_13<-data_new[data_new$WH_ID=="US13",]
data_16<-data_new[data_new$WH_ID=="US16",]
data_19<-data_new[data_new$WH_ID=="US19",]

data_13_new <- aggregate(x = data_13[c("TOTAL_CASES","LOOSE_CASES","FULL_PALLETS")],
                         FUN = sum,
                         by = list(Group.date = data_13$TRNDTE))
data_13_new$TOTAL_CASES<-as.integer(data_13_new$TOTAL_CASES)
data_13_new$LOOSE_CASES<-as.integer(data_13_new$LOOSE_CASES)
data_13_new$FULL_PALLETS<-as.integer(data_13_new$FULL_PALLETS)


data_16_new<- aggregate(x = data_16[c("TOTAL_CASES","LOOSE_CASES","FULL_PALLETS")],
                        FUN = sum,
                        by = list(Group.date = data_16$TRNDTE))

data_16_new$TOTAL_CASES<-as.integer(data_16_new$TOTAL_CASES)
data_16_new$LOOSE_CASES<-as.integer(data_16_new$LOOSE_CASES)
data_16_new$FULL_PALLETS<-as.integer(data_16_new$FULL_PALLETS)

data_19_new <- aggregate(x = data_19[c("TOTAL_CASES","LOOSE_CASES","FULL_PALLETS")],
                         FUN = sum,
                         by = list(Group.date = data_19$TRNDTE))

data_19_new$TOTAL_CASES<-as.integer(data_19_new$TOTAL_CASES)
data_19_new$LOOSE_CASES<-as.integer(data_19_new$LOOSE_CASES)
data_19_new$FULL_PALLETS<-as.integer(data_19_new$FULL_PALLETS)

data_new<- aggregate(x = data_new[c("TOTAL_CASES","LOOSE_CASES","FULL_PALLETS")],
                     FUN = sum,
                     by = list(Group.date = data_new$TRNDTE))
data_new$TOTAL_CASES<-as.integer(data_new$TOTAL_CASES)
data_new$LOOSE_CASES<-as.integer(data_new$LOOSE_CASES)
data_new$FULL_PALLETS<-as.integer(data_new$FULL_PALLETS)

con <- dbConnect(odbc::odbc(), "Azure_test", uid = "sacondiscuser", 
                 pwd = "Ax#4qiZV#PQR", timeout = 10)

dbAppendTable(con, SQL("sctemp.Ob_Network_actuals"), data_new)
dbAppendTable(con, SQL("sctemp.Ob_us13_actuals"), data_13_new)
dbAppendTable(con, SQL("sctemp.Ob_us16_actuals"), data_16_new)
dbAppendTable(con, SQL("sctemp.Ob_us19_actuals"), data_19_new)


#checknetwork<-dbGetQuery(con, "select * from sctemp.Ob_Network_actuals")

#checkus13<-dbGetQuery(con, "select * from sctemp.Ob_us13_actuals")

#checkus16<-dbGetQuery(con, "select * from sctemp.Ob_us16_actuals")

#checkus19<-dbGetQuery(con, "select * from sctemp.Ob_us19_actuals")


