

*****매크로 예제 1;
%macro sort(dsn=) ; 
proc sort data=&dsn; 
by symbol name date; 
run;
%mend;

*****매크로 예제 2;
%macro load_csv(dsn, csvname) ; 
data &dsn;
  infile &csvname dsd lrecl=99999 firstobs=2;
    informat id $15. 
        bond_name $50.        issue_d yymmdd10.       maturity_d yymmdd10. 
        cpn comma2.2        issuance 15.              jongryu $30. 
        gijun_d yymmdd10.       duration comma2.6         ytm comma2.2 
        price_cpn comma12.2     price_nocpn comma12.2   ratings $4.;

  input   id        bond_name       issue_d   maturity_d    cpn           issuance 
        jongryu   gijun_d duration  ytm       price_cpn     price_nocpn   ratings;

  format  issue_d yymmdd10.       maturity_d yymmdd10.      issuance 15.        gijun_d yymmdd10. ;
run;
%mend;
%load_csv(dsn="y2008", csvname="&csv.\20080103_20090101.csv");



******MACRO 돌리는 부분*******;
*매크로 ratio_kill;
*매크로 make_data;


%macro ratio_kill; *총 2632개;
%do i=1 %to 2632;
  %put &i of 2632;
  *DATASET A03에서 
  ID = N인 회사의 YEAR 같은 PRIVATE FIRM들 불러와서 TOTALASSET 차이 가장 적은 회사 선택;
  *선택된 회사는 SAMPLE에서 제외됨;
  data a04; set a03(keep=id name code date year totalasset); if id = &i; rename totalasset=TA_public; run;
  proc sql;
    CREATE TABLE b01 as select distinct
    a.id, a.year as pub_year, b.name, b.code, b.date, b.year, b.totalasset as TA_private, a.TA_public
    FROM a04 as a, pr_tmp as b
    WHERE a.year = b.year;
  quit;
  *0과 결측값 제거;
  data b02; set b01; if TA_private ne . and TA_private ne 0; run;
  *requiring that the ratio of their total assets (TA) is less than two 
  (i.e., max(TApublic, TAprivate) / min(TApublic, TAprivate) <2) ; 
  data b03; set b02; ratio = max(TA_private , TA_public) / min(TA_private , TA_public); 
  if ratio < 2 and ratio ne 1; run;

  *if dataset b03 is empty, remove that id from the firm;
  data _NULL_;
    if 0 then set b03 nobs=num_obs;
    if num_obs = 0 then call execute ('data Pub_tmp; set Pub_tmp; if ID=&i then chk=999; run;');
    if num_obs > 0 then call execute ('%make_data(idx=&i);');
  stop;
  run;
%end;
%mend;


%macro make_data(idx=);
  data b04; set b03; abs_diff = abs(TA_private - TA_public); run;
  proc sort data=b04; by abs_diff; run;
  data b05; set b04; if _n_=1; run;
  data private_result; set private_result b05; run;
  data b06; set b05(keep=code); chk=1; run;
  proc sort data=pr_tmp; by code; run;
  data pr_tmp; merge pr_tmp b06; by code; if chk ne 1; drop chk; run;
%mend;


**** WORK 데이터셋 지우기;

proc datasets lib=work kill memtype=data;
run;
quit;



resetline;
ods html close;
ods graphics off;
ods listing;



**month별로 숫자 매기기(1,2,3,4,...);
*month변수 추가;
data _tmp; set phd.Cbreturn__cr_dur_ranked; month + 1; by id; if first.id then month=1; run;



/************************************/
/******** 데이터 불러오기 ******/
/***********************************/

%let path=F:\SAS_data\JMYB_mid\03.FF3F;
%let csv=F:\SAS_data\JMYB_mid\03.FF3F\csv;
libname ff "&path.";

data findec;
    infile "&csv.\0.fin_dec.csv" dsd lrecl=99999 firstobs=2;
    informat symbol $7. name $25. fin 1. dec 1. findec 1.; 
    input symbol name findec;
  drop fin dec;
run;

data month;
    infile "&csv.\0.month.csv" dsd lrecl=99999 firstobs=2;
    informat _name_ $5. date yymmdd10.;
    input _name_ date;
  format date yymmdd10.;
run;

data year;
    infile "&csv.\0.year.csv" dsd lrecl=99999 firstobs=2;
    informat _name_ $5. date yymmdd10.;
    input _name_ date;
  format date yymmdd10.;
run;

data per1;
  infile "&csv.\1.per_yearly.csv" dsd lrecl=99999 firstobs=2;
  informat symbol $7. name $25. y1-y16 5.2;
  input symbol name y1-y16;
run;


*****여러 파일 한꺼번에 불러오기*****;
filename year ('d:\quarter1.dat' 'd:\quarter2.dat' 'd:\quarter3.dat' 'd:\quarter4.dat');
data temp;
infile year;
input quarter sales tax expenses payroll;
run;
proc print data = temp;
run;


**** PROC SORT nodupkey ****;

proc sort data=TR_WITH_GONGSHI_KS01 nodupkey; by id; run;
proc sort data=tr_with_gongshi_kq01 nodupkey; by id; run;
proc sort data=tr_with_gongshi_all_ks01 nodupkey; by id; run;
proc sort data=tr_with_gongshi_all_kq01 nodupkey; by id; run;


*****PROC FREQ ****;

*전체 거래 월별빈도 계산;
proc freq data=t01 noprint; tables info1 / out=t04 ; 
proc sort data=t04;  by descending percent ; run;
data t001; set t01; format trade_date yymmn6.; run;
data t001_pos; set t001; if changeshare > 0; data t001_neg; set t001; if changeshare < 0; run;
proc freq data=t001_pos noprint; tables trade_date / out=t05p(drop=PERCENT rename=(count=buy)) ;
proc freq data=t001_neg noprint; tables trade_date / out=t05n(drop=PERCENT rename=(count=sell)) ;run;
proc datasets library=work nolist;  modify t05p;  attrib _all_ label=''; quit;
proc datasets library=work nolist;  modify t05n;  attrib _all_ label=''; quit;
data t05pp; retain date; set t05p; date=put(trade_date,yymmn6.); drop trade_date;
data t05nn; retain date; set t05n; date=put(trade_date,yymmn6.); drop trade_date;
proc sort data=t05pp; by date; proc sort data=t05nn; by date;
run;

ods select none;
proc genmod data=insure;
   class car age;
   model c = car age / dist=poisson link=log offset=ln obstats;
   ods output ObStats=myObStats(keep=car age pred
                                rename=(pred=PredictedValue));
run;





/*1.proc freq로 기본정보 탐색*/

proc freq data=big01 noprint; tables info1 / out=big01_freq ; run;
proc sort data=big01_freq;  by descending percent ; run;
* proc freq 이원 분석 테이블;
proc freq data=chr01 noprint; tables chair*jooyo / out=chr01_freq ; run;
proc sort data=chr01_freq;  by descending COUNT ; run;

proc freq data=chr_q01 noprint; tables chair*jooyo / out=chr_q01_freq ; run;
proc sort data=chr_q01_freq;  by descending count ; run;



proc freq data=bigchr_sorted noprint; tables info1*chair*jooyo / out=birchr01_freq; run;
proc sort data=birchr01_freq; by descending count; run;


********PROC MEANS로 T값 구하기*********;
proc means data = Event.Event_Window noprint;
	by Event_Window;
	output out = Event.T_value mean(Abnormal_Return) = AAR mean(CAR) = CAR t(Abnormal_Return) = t_AR t(CAR) = t_CAR;
run;

****************파마맥베스Fama-Macbeth*****************;

data FM; 	set c20001; run;

proc sort data=FM;by mt;run;

%let y = fret; * define dep var here;
options nonotes;

proc reg data=FM outest=FB noprint;
	by mt;

	model &y = pret id pretxid  /adjrsq; *post ;
	model &y = pret id_amp pretxid_amp  /adjrsq;

	model &y = pret id pretxid  rc /adjrsq; *disposition (return continuity);
	model &y = pret id_amp pretxid_amp rc  /adjrsq; 

	model &y = pret id pretxid  rc bm size  /adjrsq; *all;
	model &y = pret id_amp pretxid_amp rc  bm size/adjrsq; 

quit;

** 2. drop irrelevant estimates;
proc sort data=FB; by _model_ mt; run;
data FB2; set FB; drop  &y _TYPE_  _DEPVAR_  _RMSE_ _IN_  _P_  _EDF_;
 rename _model_=model; run;

proc transpose data=FB2 out=FBny name=name prefix=coef;
 by model mt; run;
data FBny; set FBny; retain code; 
 by model mt; code=code+1; if first.mt then code=1;run;

proc sort data=FBny; by model code name;run;

** 3. Newey-West t-stat for the time-series average of coefficients;

%let lag=6;*lags for Newey-West t-stat; 
proc model data=FBny; 
 by model code name;
 parms a; exogenous coef1 ; 
 instruments / intonly;
 coef1 =a; 
 fit coef1 / gmm kernel=(bart, %eval(&lag+1), 0);
 ods output parameterestimates=param1  fitstatistics=fitresult
 OutputStatistics=residual;
quit;

** 4. output into column table;

data param1; set param1; 
 tvalue2=put(tvalue,7.2);
 T=compress(put(tvalue2,7.2)); PARAM=compress(put(estimate,7.4));
 run;

data param1a; set param1; keep model code name coef _name_; 
 _name_='PARAM'; coef=PARAM; run;
data param1b; set param1; keep model code name coef _name_;
 _name_='T'; coef=T; run;
data param2; set param1a param1b;run;
proc sort data=param2; by  code _name_ model;run;

proc transpose data=param2 out=param3; 
 by code name _name_; id model; var coef; run;
data param3; set param3; if _name_='T' then do;
 code=. ;name=.;end;run;

data param1c; set param1; keep model code name coef _name_; coef=PARAM; run;
data param1d; set param1; keep model code name coef _name_; coef=T; run;
data param1d; set param1d; rename coef=t; run;

proc sort data=param1c; by model name code;run;
proc sort data=param1d; by model name code;run;
data param1e; merge param1c param1d;by model name code;run;

proc transpose data=param1e out=param5; 
 by model ; id name; var coef t; run;

 data param6; retain model intercept id pret pretxid id_amp pretxid_amp	rc	bm	size	turn	ivol	D	MAX	pretxrc	pretxbm	pretxsize	pretxD	pretxMAX	pretxivol	pretxturn	_ADJRSQ_ _RSQ_; set param5; RUN;

options notes;

****************파마맥베스Fama-Macbeth 끝!****************;



*가중평균;

proc means data= have;
    by Date ID;
    var Diam/weight=frequency; *가중평균값 만드는 법;
    var diam_unwt;
    output out = m_diam;
run;



*PROC RANK 예제;
proc rank data=beta_pre out=portfolio_pre groups = 20 ;  * 20 portfolios ;
    var pre_beta;
    ranks pf_group;              * then, the name of ranking column is pfo-group ;
run;

*듀레이션 rank 매기기;
*1. month=1 일때 듀레이션 값으로 계산;
* 듀레이션은 채권 가격이 처음으로 평가됬을 때의 듀레이션을 기준으로 하여 매 월마다 high, middle, low 기준으로 3, 2, 1의 rank를 매김;
data _dur; set b02; by id; if first.id;  run;
proc sort data=_dur; by date; run; 
proc rank data=_dur out=_dur_ranked groups=3 ;  var duration; by date; ranks duration_rank;       run;
*label 제거;  proc datasets library=work nolist;  modify _dur_ranked;  attrib _all_ label=''; quit;
data _dur_ranked; set _dur_ranked; duration_rank + 1; run;
data _dur_ranked; set _dur_ranked; keep id duration_rank; run;

  


*TABLE 2 PANEL B 상관계수 만들기;

*상관계수 출력;
ods output PearsonCorr=e03_corr_p;
proc corr data=e01 nomiss outp=tmp;
   var inst_buysell -- amihud; run;

data e03_corr_p2; set e03_corr_p; varkey=_n_;run;
data e03_corr_p2; retain variable varkey;set e03_corr_p2; run;
data e041; set e03_corr_p2; drop pinst_buysell -- pamihud; key=1;run;
data e042; set e03_corr_p2; drop inst_buysell -- amihud; key=2;run;
data e043; set e042; variable="pvalue";
rename Pinst_buysell=inst_buysell	Psize=size	Pbm=bm	Pdebt=debt
	Proe=roe		Pbeta=beta	Pstd_return=std_return		Pamihud=amihud; run;
	
data e05; set e041 e043; run;
proc sort data=e05; by varkey key; run;
data ev.Table2_Panel_b; set e05; drop varkey key; run;



proc sql;

  create table edset as
  select distinct   event_num, 
                      event.cd as cd, 
                      stockret_w_mkt.dt as dt, 
                      ret, 
                      mkt_ret, 
                      stockret_w_mkt.seq_day - event.seq_day as event_day
  
  from  eventdset   as event, dsetwithmkt as stockret_w_mkt

  where event.cd eq stockret_w_mkt.cd 

  order by event_num, event_day ; 
quit; run;


*MACRO TRANWRD 글자 바꾸기 글자바꾸기;
%macro aar(in=);
%let out = %sysfunc(tranwrd(%str(&in),car,aar));


%mend;



*length 및 매크로 macro 예제;
ods graphics off;
ods html close; 
data _est_car_result; set _null_; run;

%macro car_ttest(in=);
proc means data=&in noprint;  var car; output out=_est(drop=_type_ _freq_) n=n mean=car; run;
proc ttest data=&in; var car; ods output ttests=_t(drop=df); run; 
proc datasets library=work nolist;  modify _t;  attrib _all_ label=''; quit;
data _tmp; merge _est _t(keep=tvalue); run;
data _tmp; retain name ; length name $50.; name="&in"; set _tmp;run;
data _est_car_result; set _est_car_result _tmp; run;
%mend;



*매 row마다 숫자 더하기;

data students1;
  set students;
  count + 1;
  by gender;
  if first.gender then count = 1;
run;



*SAS데이터셋 용량 줄이는 매크로(변수길이 줄이기) 다이어트;

%macro change(dsn);                                         
                                                            
data _null_;                                                
  set &dsn;                                                 
  array qqq(*) _character_;                                 
  call symput('siz',put(dim(qqq),5.-L));                    
  stop;                                                     
run;                                                        
                                                            
data _null_;                                                
  set &dsn end=done;                                        
  array qqq(&siz) _character_;                              
  array www(&siz.);                                         
  if _n_=1 then do i= 1 to dim(www);                        
    www(i)=0;                                               
  end;                                                      
  do i = 1 to &siz.;                                        
    www(i)=max(www(i),length(qqq(i)));                      
  end;                                                      
  retain _all_;                                             
  if done then do;                                          
    do i = 1 to &siz.;                                      
      length vvv $50;                                       
      vvv=catx(' ','length',vname(qqq(i)),'$',www(i),';');  
      fff=catx(' ','format ',vname(qqq(i))||' '||           
          compress('$'||put(www(i),3.)||'.;'),' ');         
      call symput('lll'||put(i,3.-L),vvv) ;                 
      call symput('fff'||put(i,3.-L),fff) ;                 
    end;                                                    
  end;                                                      
run;                                                        
                                                            
data &dsn._;                                                
  %do i = 1 %to &siz.;                                      
    &&lll&i                                                 
    &&fff&i                                                 
  %end;                                                     
  set &dsn;                                                 
run;                                                        
                                                            
%mend;                      

%change(work.b02);           



****** WINSORIZE하는 파일;

/*winsorize <0.5percentile(0.005) >99.5 percentile(0.995) values*/
proc univariate data=datatowinsorize /*datatowinsorize<--d2w*/ noprint ;
  by cmon ;
  var variabletowinsorize ;
  output out = datatowinsorize_extreme 
  pctlpre = win 
  pctlpts= 0.5 99.5 ; *이 숫자들 바꾸면 다른 백분위로 winsorize 가능;
run ;

data d2w_temp;
  merge datatowinsorize datatowinsorize_extreme;
  by date; *이 코드에서는 날짜변수 사용. 다른 변수를 by문에 넣거나 아예 by문 안써도 무방;
run;
/*datatowinsorize_extreme 값이 하나일때 사용하는 대안*/
/*
data d2w_temp;
 if _n_=1 then set datatowinsorize_extreme;
 set datatowinsorize;
run;
*/
data d2w_temp;
  set d2w_temp;
  if variabletowinsorize =. then delete; /*무응답 제거*/
run;
data d2w_temp;
  set d2w_temp;
  if variabletowinsorize <= win0_5 then variabletowinsorize=win0_5; /*winsorize(극단 하위값 교체)*/
  if variabletowinsorize >= win99_5 then variabletowinsorize=win99_5; /*winsorize(극단 상위값 교체)*/
run; 
data datawinsorized; /*최종데이터 생성(win0.5, win99.5변수 제거)*/
  set d2w_temp;
  drop win0_5 win99_5;
run; 

/*winsorize <1percentile(0.01) >99 percentile(0.99) values*/

proc univariate data=C01_m_1ratings01 noprint ; var rirf1 ;  output out = C01_m_1ratings01_tmp   pctlpre = p   pctlpts= 1 99 ; run ;
data C01_m_1ratings01_tmp; set C01_m_1ratings01_tmp; chk=1; run;
data C01_m_1ratings01_; set C01_m_1ratings01; chk=1; run;
data C01_m_1ratings01; merge C01_m_1ratings01_ C01_m_1ratings01_tmp; by chk; 
  if rirf1 < p1 then rirf1=p1;    if rirf1 > p99 then rirf1=p99;    
  drop chk p1 p99;  run;

/* SAS 문자 숫자 합치기
When a number and a character are concatenated, the number is converted to a character variable first, then the two character variables are concatenated together. The default format for converting a numeric variable to a character variable is BEST12. (although that can vary based on the format of your numeric variable).  put(30,BEST12.) would yield '          30' which is then concatenated to the character variable.

To avoid this, either use strip as Aaron notes, or do your concatenation using CATS (res=cats(name,age);) which automatically strips all variables, or put the numeric variable yourself (and with PUT, you can forcibly left-justify it if you want with the -l option).
*/



**daily를 MONTHLY로 바꿈(daily to monthly) - 월별 마지막 날 데이터만 남김(id,year,month 합친 변수 만든 후 last만 남기는 식으로);

data c02; set c01; gijun_m=month(gijun_d); run;
data c03; set c02; id_year_month=cats(id,'-',year,gijun_m); run;
proc sort data=c03; by id_year_month;
data c04; set c03; by id_year_month; if last.id_year_month then chk=1; if duration ne 0; run;
data c05; set c04; if chk=1; drop gijun_m -- chk; run;


/*
HISTOGRAM Statement
(http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect013.htm)

HISTOGRAM <variables> < / options> ;*/
   proc univariate data=Steel;
      histogram;
   run;
*Likewise, the following statements create histograms for Length and Width:
   proc univariate data=Steel;
      var Length Width;
      histogram;
   run;
*The following statements create a histogram for Length only:
   proc univariate data=Steel;
      var Length Width;
      histogram Length / endpoints = 3.425 to 3.6 by .025;

   run;






/*******************************************************************************/
/**                                  경영대 석박사과정을 위한 DB 및 SAS 강의                              **/
/**                                                                                                                             **/
/**                                     Fama and French, 3 Factor Regression                         **/
/**      "Common risk factors in the returns on stocks and bonds(1993, JFE)"            **/
/**                                                               2015.spring                                         **/
/*                         modified Eom Yun-Sung V1.0  by  Kim Ryumi                               **/ 
/*   1. financial data: TS-2000                                           */
/*      * 2010-2013: 제조업, 모든 결산 법인   (단위 천원)                            */
/*   2. stock price data: dataguide                                                 */
/*      * 2010 ~ 2013: monthly                                            */
/*      * symbol date share end_pr size ret                         */
/*   3. index data: dataguide                                                    */
/*      * 2010 ~ 2013: monthly                                            */
/*      * date, Rm, Rf                                                   */
/*   4. grouping: independent variables                                   */
/*      * SMB: small(50%), big(50%)                                       */
/*      * HML: low(30%), medium(40%), high(30%)                           */
/*   5. mimicking portfolios : independent variables                      */
/*      * Rm - Rf                                                         */
/*      * SMB                                                             */ 
/*      * HML                                                             */
/*   6.  grouping: dependent variables                                    */
/*      * Ri - Rf : 25 portfolios                                         */
/*      * grouping by SIZE, BEME                                          */
/*   7. regression                                                        */ 
/*      * Ri(t) - Rf(t) = a + b*(Rm - Rf)(t)+ s*SMB(t) + h*HML(t) + e(t)  */

/*******************************************************************************/


%let path=F:\SAS_data\3Factor;

libname ff "&path.";

/**********************************************
      1. financial data: TS-2000

   *   2010-2013: 제조업, 모든 결산 법인
***********************************************/

/*deprecated -- 엑셀파일 import에 문제 있음
proc import out= a1 
            datafile= "&path.\ts2000.csv" 
            dbms=csv replace;
     getnames=yes;
run;
*/

 data WORK.A1    ;
 %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
 infile 'F:\SAS_data\3Factor\ts2000.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
    informat VAR1 $13. ;
    informat VAR2 $7. ;
    informat VAR3 $7. ;
    informat VAR4 best32. ;
    informat VAR5 best32. ;
    informat VAR6 best32. ;
    informat VAR7 best32. ;
    informat VAR8 best32. ;
    informat VAR9 best32. ;
    format VAR1 $13. ;
    format VAR2 $7. ;
    format VAR3 $7. ;
    format VAR4 best12. ;
    format VAR5 best12. ;
    format VAR6 best12. ;
    format VAR7 best12. ;
    format VAR8 best12. ;
    format VAR9 best12. ;
 input
             VAR1 $
             VAR2 $
             VAR3
             VAR4
             VAR5
             VAR6
             VAR7
             VAR8
             VAR9
 ;
 if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
 run;


data financial;set a1;
  rename VAR1=name VAR2=code VAR3=fis_year VAR4=debt VAR5=capital VAR6=prefer_capital VAR7=common_num VAR8=eps VAR9=fis_end_price;run;
data financial;set financial;
    year=substr(fis_year,1, 4);     fis=substr(fis_year,6, 2);   /* 년도와 결산월 추출    */RUN;
data financial;set financial;
  if fis ne 12 then delete;       /* 12월 결산법인만      */
  year1=year + 1;   /* 그 다음해로 assign   */
run;
data financial;set financial;
  BE=(capital - prefer_capital)*1000;  /* book value(원으로 환산)           */
  ME=common_num*fis_end_price;   /* market value         */
  BEME=BE/ME;                  /* book to market ratio */
  drop fis_year fis;
run;




/**********************************************
      2. stock price data : dataguide

  *  2010-2013: monthly
  *  symbol date share(주) end_pr(원) size(백만원) ret(%) code
***********************************************/
data stock ; set ff.stock; run;
data stock; set stock; rename symbol=code; run;
/* deprecated(csv파일 000010 --> A000010으로 바꾸었기 때문)
code=substr(symbol, 2, 6); drop symbol; run; */





/**********************************************
      3. index data: dataguide

  *  2010-2013: monthly
  *  date, Rm, Rf
***********************************************/
data index;  set ff.index;/* KOSPI RET, 국채 3년*/
 rf=((1+rf_y/100)**(1/12)-1)*100; /* monthly interest rate(%)로 환산*/
  rm_rf= rm - rf;
  drop rf_y;
run;

/**********************************************
      4. grouping: independent variables
*  SMB: small(50%), big(50%)
*  HML: low(30%), medium(40%), high(30%)
***********************************************/
/*** SMB ***/
data d1;
  set stock(keep=date code share end_pr size);
  SIZE1 = share * end_pr; /*t시가총액 항목 없을 경우*/
  year=year(date); month =month(date);
  if month ne 6 then delete;
run;

data d2;
  set d1;
  keep date year code SIZE;
run;

/*** HML ***/

/*e1 dataset: financial dataset에서 year1, code, BMratio만 가져옴*/
data e1;  set financial(keep=year1 code BEME);
  /* delete negative BEME (자본잠식 제거)*/
  if BEME < 0 or BEME = '.' then delete; 
  rename year1=year; /* 회계연도 다음해 6월 size data와 merge 하기 위해 */
run;

/*** merge ***/
/*이중  sort 가능 -- proc sort data=dataset; by sortvar1 sortvar2; run; 이런 식으로.
마찬가지로 이중 merge도 가능 -- data newdata; merge data1 data2; by sortvar1 sortvar2; run;*/
proc sort data=d2; by year code; run;
proc sort data=e1; by year code; run;

data f1; merge d2 e1; by year code; run;

data f2;  /* final sample data */
  set f1;
  /*BMration ,Size에 대한 무응답 제거*/
  if BEME eq '.' then delete;  /* delete company(no BEME) */
  if SIZE eq '.' then delete; 
run; 



*if dataset b03 is empty, remove that id from the firm;
data _NULL_;
  if 0 then set b03 nobs=num_obs;
  if num_obs = 0 then call execute ('%put empty dataset;');
  if num_obs > 0 then call execute ('%put non-empty dataset;');
stop;
run;


/*** grouping by SIZE, BEME ***/
/*rank문 사용; 
proc rank data=inputdata out=outputdata groups=2(median) 3(tertile) 4(quantile) 5(quintile) ...;
by date(group할 변수) var size(rank 매길 변수) ranks big(rank 매긴 값(1,2 등등) 넣을 변수)*/
proc rank data=f2 out=f3 groups=2;
  by date;
  var SIZE;
  ranks big;
run;

proc rank data=f3 out=f4 groups=10;
  by date;
  var BEME;
  ranks high1;
run;

data f5;
  set f4;
  if high1 <= 2 then high=0;
  else if high1 le 6 then high=1;
  else if high1 le 9 then high=2;
run;

data f6;  /* BIG, SMALL // LOW, MEDIUM, HIGH */
  set f5;
  keep code big high year;
run;


/*******************************************************
      5. mimicking portfolios : independent variables

*  Rm - Rf
*  SMB
*  HML
********************************************************/

data g1; /*g 붙은 데이터셋은 factor-mimicking 포트폴리오*/
  set stock;
  keep code date ret;
  /* 2011년 7월 이전의 데이터 모두 제거 -- MDY함수 써도 될거같음*/
  if year(date) lt 2011 then delete;  /* 2011.07 - 2013.12 */
  if year(date) eq 2011 and month(date) lt 7 then delete;
  
run;

/*g2 데이터셋 --> 7월에서 12월까지의 데이터에 대해서 year 변수를 1 줄임
이로 인해서 2011.7~2012.6은 2011.7 ~ 2011.1, 2011.2, 2011.3, 2011.4, 2011.5, 2011.6으로 바뀜*/
data g2;
  set g1;
  if month(date) lt 7 then year=year(date)-1; else year=year(date);
run;

proc sort data=f6; by code year; run;
proc sort data=g2; by code date; run;

/*g3: g2 데이터셋과 f6 데이터셋을 합친 데이터셋*/
data g3;
  merge g2 f6;
  by code year; 
  if big eq '.' then delete;
run;

/*g4: g3 데이터셋을 6개의 그룹으로 분리하고 숫자 1(소형, 가치주)부터 6(대형, 성장주)까지 부여*/
data g4;
  set g3;
  if big eq 0 and high eq 0 then group=1;
  if big eq 0 and high eq 1 then group=2;
  if big eq 0 and high eq 2 then group=3;
  if big eq 1 and high eq 0 then group=4;
  if big eq 1 and high eq 1 then group=5;
  if big eq 1 and high eq 2 then group=6;
run;


/* group 별 equal weight average */
proc means data=g4 noprint;
  /*proc means의 class문 --> by 문 대체가능!!*/
  class date group;/* by 로 해도 되지만 정렬되있어야함*/
  var ret;
  output out=h1;
run;

data h2;
  set h1;
  if _stat_ ne 'MEAN' then delete;
  if date eq '.' then delete;
  if group eq '.' then delete;
run;
/* group 별 dataset 따로 만들기*/
%macro aaa;
%do i=1 %to 6;
data j&i(keep=date ret&i);
  set h2;
  if group ne &i then delete;
  rename ret=ret&i;
run;
%end;
%mend;
%aaa; /*macro aaa 실행문*/

data k1;
  merge j1 j2 j3 j4 j5 j6 ;
run;

/*k2 데이터셋 --> SMB, HML 팩터 수익률 구함*/
data k2;
  set k1;
  SMB = (ret1 + ret2 + ret3)/3 - (ret4 + ret5 + ret6)/3; 
  HML = (ret3 + ret6)/2 - (ret1 + ret4)/2; 
run;

/*mimicking 데이터셋 --> rmrf(index)와 SMB, HML(k2)를 합쳐서 최종적인
factor-mimicking 포트폴리오의 수익률 데이터셋 구성*/
data mimicking;  /* mimicking portfolios */
  merge index k2;
  by date;
  if year(date) lt 2011 then delete;  /* 2011.07 - */
  if year(date) eq 2011 and month(date) lt 7 then delete;
  keep date rm_rf SMB HML;
run;


/**********************************************
      6.  grouping: dependent variables

*  Ri - Rf : 25 portfolios
***********************************************/
/***re-grouping by SIZE, BEME ***/
proc rank data=f2 out=f3 groups=5;
  by date;
  var SIZE;
  ranks big;
run;

proc rank data=f3 out=f4 groups=5;
  by date;
  var BEME;
  ranks high;
run;

data ff6;  /* SIZE(1-5) // BEME(1-5) */
  set f4;
  keep code big high year;
run;

proc sort data=ff6; by code year; run;
proc sort data=g2;/* g2는 year 조정해둔 stock return dataset*/   by code date ; run;

data gg3;
  merge g2 ff6;
  by code year;
  if big eq '.' then delete;
run;

proc sort data=gg3;   by date; run;
proc sort data=index; by date; run;

data gg31;  
  merge gg3 index;
  by date;
  excess= ret - rf;
  if excess eq '.' then delete;
run;

/* 25개 portfolio 번호 매기기 */

data gg4;
  set gg31;
  /*do i=0 to 4; for문과 동일함*/
  do i=0 to 4;
      do j=0 to 4;
        if big = i and high = j then group=i*5+ j+1;
      end;
  end;
run;
/*
data gg5;
  set gg31;
  *do i=0 to 4; for문과 동일함;
  do i=0 to 4;
      do j=0 to 4;
        if big = i and high = j then group=i*5+ j+1;
      end;
  end;
  drop i j;
run;
*/

/* 25개 group 별 excess return의  equal weight average */
proc means data=gg4 noprint;
  class date group;
  var excess;
  output out=hh1;
run;

data hh2;
  set hh1;
  if _stat_ ne 'MEAN' then delete;
  if date eq '.' then delete;
  if group eq '.' then delete;
run;

%macro aaaa;

%do i=1 %to 25;

    data jj&i(keep=date excess&i);
      set hh2;
        if group ne &i then delete;
        rename excess=excess&i;
    run;

%end;
%mend;
%aaaa;
data dependent;
  merge jj1 jj2 jj3 jj4 jj5 jj6 jj7 jj8 jj9 jj10
          jj11 jj12 jj13 jj14 jj15 jj16 jj17 jj18 jj19 jj20
          jj21 jj22 jj23 jj24 jj25;
run;


/***************************************************************
      7. regression 

* Ri(t) - Rf(t) = a + b*(Rm - Rf)(t)+ s*SMB(t) + h*HML(t) + e(t)
*****************************************************************/

data m1;
  merge dependent mimicking;
  by date;
run;

/*매크로 결과 넣을 result 데이터셋 생성*/
data result;
  set _null_;
run;

%macro reg;

%do i=1 %to 25;

  proc reg data=m1 outest=result&i tableout noprint; 
    model excess&i = rm_rf SMB HML              /adjrsq;
  run; quit;
  
  data result_new&i;
    set result&i;
      if _type_ ne 'PARMS' and _type_ ne 'T' then delete;
  run;
  
  data result;
    set result result_new&i;
  run;

%end;

%mend;

%reg;


data result_new;
  set result;
  if _TYPE_ ne 'PARMS' then delete;
run;

proc means data=result_new mean t;
  var intercept rm_rf smb hml _RSQ_ _ADJRSQ_;
run;

proc export data=result dbms=excel
  outfile="d:\ff3\result" replace;
run;

/******************************** End of Program *******************************/




%macro rolling_beta();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    *1) Partial dataset 만듦;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) partial dataset의 각 id별 갯수가 23개 미만일 경우 제거;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;

    * 2) Partial에 대해 회귀모형 돌려서 beta 구함;
    *Ri - Rf ~ DEF, TERM 회귀모형 돌림;
    *C01_reg에 DEF, TERM 베타값 저장;
    *C01_resid에 잔차값(e_t-) 저장;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = def term;  by id;
      output out = c01_resid R = resid;  
    run; quit;

    *C01_beta에 TERM, DEF 베타 변수명 바꿔서(def_beta, term_beta) 저장;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename def=def_beta term=term_beta; run;
    proc sort data=c01_beta; by id; run;

    * 3) 2)에서 구한 beta로 24+1 ... 60+1 개월에 해당하는 데이터 불러와서 cross section 회귀식;
    * 3-1) 24+1개월에 해당하는 데이터 불러온 후 베타와 합치기;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = def_beta term_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(def_beta)=def_beta t(def_beta)=def_beta_t 
  mean(term_beta)=term_beta t(term_beta)=term_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t;  attrib _all_ label=''; quit;

%mend;

%macro rolling_beta_liq();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = def term liq;  by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename def=def_beta term=term_beta liq=liq_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = def_beta term_beta liq_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_liq 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(def_beta)=def_beta t(def_beta)=def_beta_t 
  mean(term_beta)=term_beta t(term_beta)=term_beta_t
  mean(liq_beta)=liq_beta t(liq_beta)=liq_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_liq;  attrib _all_ label=''; quit;

%mend;

%macro rolling_beta_gdp_inf();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = def term gdp inf;  by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename def=def_beta term=term_beta gdp=gdp_beta inf=inf_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = def_beta term_beta gdp_beta inf_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_gdp_inf
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(def_beta)=def_beta t(def_beta)=def_beta_t 
  mean(term_beta)=term_beta t(term_beta)=term_beta_t 
  mean(gdp_beta)=gdp_beta t(gdp_beta)=gdp_beta_t 
  mean(inf_beta)=inf_beta t(inf_beta)=inf_beta_t ; 
run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_gdp_inf;  attrib _all_ label=''; quit;

%mend;

%macro rolling_beta_liq_inf();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = def term liq inf;  by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename def=def_beta term=term_beta liq=liq_beta inf=inf_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = def_beta term_beta liq_beta inf_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_liq_inf
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(def_beta)=def_beta t(def_beta)=def_beta_t 
  mean(term_beta)=term_beta t(term_beta)=term_beta_t 
  mean(liq_beta)=liq_beta t(liq_beta)=liq_beta_t 
  mean(inf_beta)=inf_beta t(inf_beta)=inf_beta_t ; 
run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_liq_inf;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_gdp_all();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = def term liq gdp inf;  by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename def=def_beta term=term_beta liq=liq_beta gdp=gdp_beta inf=inf_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = def_beta term_beta liq_beta gdp_beta inf_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t__all
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(def_beta)=def_beta t(def_beta)=def_beta_t 
  mean(term_beta)=term_beta t(term_beta)=term_beta_t 
  mean(liq_beta)=liq_beta t(liq_beta)=liq_beta_t
  mean(gdp_beta)=gdp_beta t(gdp_beta)=gdp_beta_t
  mean(inf_beta)=inf_beta t(inf_beta)=inf_beta_t ; 
run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t__all;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_liq_o();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = liq; by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename liq=liq_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = liq_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_liq_o 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(liq_beta)=liq_beta t(liq_beta)=liq_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_liq_o;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_gdp_o();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = gdp; by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename gdp=gdp_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = gdp_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_gdp_o 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(gdp_beta)=gdp_beta t(gdp_beta)=gdp_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_gdp_o;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_inf_o();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = inf; by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename inf=inf_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = inf_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_inf_o 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(inf_beta)=inf_beta t(inf_beta)=inf_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_inf_o;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_liq_inf_o();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    *1) ;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;

    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = liq inf; by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename liq=liq_beta inf=inf_beta; run;
    proc sort data=c01_beta; by id; run;

    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = liq_beta inf_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_liq_inf_o 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(liq_beta)=liq_beta t(liq_beta)=liq_beta_t 
  mean(inf_beta)=inf_beta t(inf_beta)=inf_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_liq_inf_o;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_liq_gdp_o();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    *1) ;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;

    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = liq gdp; by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename liq=liq_beta gdp=gdp_beta; run;
    proc sort data=c01_beta; by id; run;

    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = liq_beta gdp_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_liq_gdp_o 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(liq_beta)=liq_beta t(liq_beta)=liq_beta_t 
  mean(gdp_beta)=gdp_beta t(gdp_beta)=gdp_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_liq_gdp_o;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_gdp_inf_o();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    *1) ;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;

    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = gdp inf; by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename gdp=gdp_beta inf=inf_beta; run;
    proc sort data=c01_beta; by id; run;

    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = gdp_beta inf_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t_gdp_inf_o 
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(gdp_beta)=gdp_beta t(gdp_beta)=gdp_beta_t 
  mean(inf_beta)=inf_beta t(inf_beta)=inf_beta_t ; run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t_gdp_inf_o;  attrib _all_ label=''; quit;

%mend;


%macro rolling_beta_all_o();
data e01__result; set _null_; run; *최종 데이터 저장;
  %DO idx=1 %to 37;
    data c01; set b01; if month ge &idx and month le &idx+23; run;
    *1-1) ;
    proc freq data=c01 noprint; tables id / out=c02; run;
    data c03; set c02; if count < 23; chk=99; drop count percent; run;
    proc sort data=c01; by id; proc sort data=c03; by id; run;
    data c01; merge c01 c03; by id; if chk ne 99; drop chk; run;
    * 2) ;
    proc sort data=c01; by id; run;
    proc reg data=c01 outest=c01_reg edf tableout noprint;
      model rirf = liq gdp inf; by id;
      output out = c01_resid R = resid;  
    run; quit;
    data c01_beta; set c01_reg; if _type_='PARMS'; drop intercept _model_ _depvar_ _rsq_ rirf _type_; 
    rename liq=liq_beta gdp=gdp_beta inf=inf_beta; run;
    proc sort data=c01_beta; by id; run;
    * 3) ;
    data d01; set b01; if month = &idx+24; keep id--cpn date year month rirf; run;
    proc sort data=d01; by id; run;
    data e01; merge d01 c01_beta; by id; if bond_name ne ""; run;
    data e01__result; set e01__result e01; run; 
    %PUT &idx;
  %END;

proc reg data=e01__result outest=e01_reg_b edf tableout noprint;
  model rirf = liq_beta gdp_beta inf_beta; by month;
  output out = e01_resid_b R = resid;   
run; quit;
data _table_pre; set e01_reg_b; if _TYPE_="PARMS" ; run;

proc means data=_table_pre noprint; output out = _table_pre_t__all_o
  mean(_rsq_)=_rsq_ t(_rsq_)=_rsq__t 
  mean(intercept)=intercept t(intercept)=intercept_t 
  mean(liq_beta)=liq_beta t(liq_beta)=liq_beta_t
  mean(gdp_beta)=gdp_beta t(gdp_beta)=gdp_beta_t
  mean(inf_beta)=inf_beta t(inf_beta)=inf_beta_t ; 
run;

*label 제거;  proc datasets library=work nolist;  modify _table_pre_t__all_o;  attrib _all_ label=''; quit;

%mend;



options nonotes;

%rolling_beta();
%rolling_beta_liq();
%rolling_beta_gdp_inf();
%rolling_beta_liq_inf();
%rolling_beta_gdp_all();


%rolling_beta_liq_o();
%rolling_beta_gdp_o();
%rolling_beta_inf_o();

%rolling_beta_liq_inf_o();
%rolling_beta_liq_gdp_o();
%rolling_beta_gdp_inf_o();
%rolling_beta_all_o();

options notes;

*합치기;
data __tpt1; retain name; length name $20.; name="TERMDEF"; set _table_pre_t; run;
data __tpt2; retain name; length name $20.; name="LIQ"; set _table_pre_t_liq_o; run;
data __tpt3; retain name; length name $20.; name="GDP"; set _table_pre_t_gdp_o; run;
data __tpt4; retain name; length name $20.; name="INF"; set _table_pre_t_inf_o; run;

data __tpt5; retain name; length name $20.; name="LIQINF"; set _table_pre_t_liq_inf_o; run;
data __tpt6; retain name; length name $20.; name="LIQGDP"; set _table_pre_t_liq_gdp_o; run;
data __tpt7; retain name; length name $20.; name="GDPINF"; set _table_pre_t_gdp_inf_o; run;
data __tpt8; retain name; length name $20.; name="LIQGDPINF"; set _table_pre_t__all_o; run;

data __tpt9; retain name; length name $20.; name="TERMDEF_LIQ"; set _table_pre_t_liq; run;
data __tpt10; retain name; length name $20.; name="TERMDEF_GDPINF"; set _table_pre_t_gdp_inf; run;
data __tpt11; retain name; length name $20.; name="TERMDEF_LIQINF"; set _table_pre_t_liq_inf; run;
data __tpt12; retain name; length name $20.; name="TERMDEF_ALL"; set _table_pre_t__all; run;





data _table__; set __tpt1 __tpt2 __tpt3 __tpt4 __tpt5 __tpt6 __tpt7 __tpt8 __tpt9 __tpt10 __tpt11 __tpt12; run;









********************************************************************************************;
*[속도] SASFILE 구문을 사용하여서 메모리에 동일 데이터 세트 로딩 후 작업
********************************************************************************************;


DATA TEST; 
SET SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
SASHELP.Zipcode 
RUN; 

* 매 작업 마다 하드 디스크와 메모리 사이에 I/O 발생; 
%macro back; 
PROC SQL; 
CREATE TABLE BACK AS 
SELECT * 
FROM TEST 

%DO I=1 %TO 5; 
UNION ALL 
SELECT * 
FROM TEST 
%END; 
; 
QUIT; 
%MEND; 

%BACK; 

* 메모리에 TEST 데이터 세트를 로딩하여서 처리 후 메모리에서 제거; 
sasfile TEST open; 
%macro back; 
PROC SQL; 
CREATE TABLE BACK AS 
SELECT * 
FROM TEST 

%DO I=1 %TO 5; 
UNION ALL 
SELECT * 
FROM TEST 
%END; 
; 
QUIT; 
%MEND; 

%BACK; 
sasfile TEST close; 




/*data aa; set a; idx=0; if MDY(3, 01, 2004)<= date and date <= INTNX('month',MDY(3, 01, 2004),60) then idx =1; run;*/

%macro prepostbeta(start,end,pre,post);

data x_final_prepost_beta; set _null_; run;
data result; set _null_; run;
%let datastart = &start;
%let dataend = %eval(&end-(&pre+&post));
  /*2차 loop문 시작*/
  %DO i= &datastart %TO &dataend;
    * 2차 loop문은 (i, a, b)식으로 구성. ex: i=1980이면 (1980,1985,1990);
    %DO j= 1 %TO 12;
        %let a = %eval(&i + &pre); 
        %let b = %eval(&i + &pre + &post); 
        %put &i &a &b &j; 
        /*&i= 1980 &a=1985, &b=1990, &j= 1..12*/
        /*-- 3.1.1 dataset  --*/
        data itr;
          set fama_macbeth;
          if MDY(&j, 01, &i)<= date <= INTNX('month',MDY(&j, 01, &i), 60) then idx =0;              * select the past 5 yrs data for pre-beta ;
          if INTNX('month',MDY(&j, 01, &i), 60)< date <= INTNX('month',MDY(&j, 01, &i), 120) then idx =1; * select following 5 yrs for post-beta ;
          *if &i <= year < &a then idx=0; * select the past 5 yrs data for pre-beta ;
          *if &a <= year < &b then idx=1; * select following 5 yrs for post-beta ;
          if rirf=. then delete;           * deleting data with missing value;
          where &i <= year < &b;
          proc sort data=itr; by symbol; 
        run;



data long2; 
  input famid year faminc spend ; 
cards; 
1 96 40000 38000 
1 97 40500 39000 
1 98 41000 40000 
2 96 45000 42000 
2 97 45400 43000 
2 98 45800 44000 
3 96 75000 70000 
3 97 76000 71000 
3 98 77000 72000 
; 
run ;

proc transpose data=long2 out=widef prefix=faminc;
   by famid;
   id year;
   var faminc;
run;

proc transpose data=long2 out=wides prefix=spend;
   by famid;
   id year;
   var spend;
run;

data wide2;
    merge  widef(drop=_name_) wides(drop=_name_);
    by famid;
run;

proc print data=wide2;
run;










/******RESTART******/
resetline; ods html close; ods graphics off; ods listing;
/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";



data _FY_REPORTS_2011_2016;
    infile "&path.\FY_CONSENSUS_2011-2016.csv" dsd lrecl=99999 firstobs=2;
INFORMAT ID 7. date yymmdd10. symbol $8. name $30. gubun $20. report_issuer $20. analyst $12.
consensus_status $20. gyulsan 8. FY $6. TP 10. SALES 20. OP 20. NI 20. EPS BEST15. NI_CONTROL 20. EPS_CONTROL BEST15.;
INPUT ID date symbol name gubun report_issuer analyst consensus_status gyulsan FY TP SALES OP NI EPS NI_CONTROL EPS_CONTROL;
format date yymmdd10.;
RUN;

data _FQ_REPORTS_2011_2016;
    infile "&path.\FQ_CONSENSUS_2011-2016.csv" dsd lrecl=99999 firstobs=2;
INFORMAT ID 7. date yymmdd10. symbol $8. name $30. gubun $20. report_issuer $20. analyst $12.
consensus_status $20. gyulsan 8. FQ $6. TP 10. SALES 20. OP 20. NI 20. EPS BEST15. NI_CONTROL 20. EPS_CONTROL BEST15.;
INPUT ID date symbol name gubun report_issuer analyst consensus_status gyulsan FQ TP SALES OP NI EPS NI_CONTROL EPS_CONTROL;
format date yymmdd10.;
RUN;

proc sort data=_FY_REPORTS_2011_2016 out=A001 nodupkey; by date symbol name gubun report_issuer analyst consensus_status FY; run;
proc transpose data=a001 out=a001_tr;
by date symbol name gubun report_issuer analyst consensus_status; id FY; 
var gyulsan TP SALES OP NI EPS NI_CONTROL EPS_CONTROL; run;


proc sort data=_FQ_REPORTS_2011_2016 out=B001 nodupkey; by date symbol name gubun report_issuer analyst consensus_status FQ; run;
proc transpose data=B001 out=B001_tr;
by date symbol name gubun report_issuer analyst consensus_status; id FQ; 
var gyulsan TP SALES OP NI EPS NI_CONTROL EPS_CONTROL; run;


data C001; set A001(drop=id rename=(FY=FYFQ)) B001(drop=id rename=(FQ=FYFQ)); run;
proc sort data=C001; by date symbol name gubun report_issuer analyst consensus_status gyulsan; run;

data c002; set c001; run;
proc sort data=c002; by name analyst gyulsan; run;
/*RESTART*
data con.FYFQ_analyst_report; set c002; run;
*/

*sample 축소 필요!;

data c002; set con.FYFQ_analyst_report; run;




*SAS데이터셋 용량 줄이는 매크로(변수길이 줄이기) 다이어트;

%macro change(dsn);                                         
                                                            
data _null_;                                                
  set &dsn;                                                 
  array qqq(*) _character_;                                 
  call symput('siz',put(dim(qqq),5.-L));                    
  stop;                                                     
run;                                                        
                                                            
data _null_;                                                
  set &dsn end=done;                                        
  array qqq(&siz) _character_;                              
  array www(&siz.);                                         
  if _n_=1 then do i= 1 to dim(www);                        
    www(i)=0;                                               
  end;                                                      
  do i = 1 to &siz.;                                        
    www(i)=max(www(i),length(qqq(i)));                      
  end;                                                      
  retain _all_;                                             
  if done then do;                                          
    do i = 1 to &siz.;                                      
      length vvv $50;                                       
      vvv=catx(' ','length',vname(qqq(i)),'$',www(i),';');  
      fff=catx(' ','format ',vname(qqq(i))||' '||           
          compress('$'||put(www(i),3.)||'.;'),' ');         
      call symput('lll'||put(i,3.-L),vvv) ;                 
      call symput('fff'||put(i,3.-L),fff) ;                 
    end;                                                    
  end;                                                      
run;                                                        
                                                            
data &dsn._;                                                
  %do i = 1 %to &siz.;                                      
    &&lll&i                                                 
    &&fff&i                                                 
  %end;                                                     
  set &dsn;                                                 
run;                                                        
                                                            
%mend;                      

/*----------------- DEPRECATED --------------;
*2013년 이후, kospi200으로 축소;
DATA _K200; SET CON.K200; drop MKTCAP; RUN;
proc sort data=c002 out=c003; by symbol; proc sort data=_k200; by symbol; run;
data c004; merge c003 _k200; by symbol; if RANK NE .; if year(date) >= 2013; run;
proc sort data=c004; by date; run;
*/

/*----------------- NEW --------------*/
*2014년 이후, kospi50으로 축소;
DATA _K50; SET CON.K50; drop NAME MKTCAP; RUN;
proc sort data=c002 out=c003; by symbol; proc sort data=_k50; by symbol; run;
data c004; merge c003 _k50; by symbol; if RANK NE .; if year(date) >= 2014; run;
proc sort data=c004; by date; run;


*report 별로 number 매기기; 
proc sort data=c004; by date symbol name report_issuer analyst; run;
data c005; set c004; by date symbol name report_issuer analyst; retain report_no 0;
if first.analyst then report_no = report_no + 1; run;
data c005; retain report_no; set c005; run;
data c0051; set c005; if analyst = "NULL" then delete; run;

/*RESTART*
data con.FYFQ_analyst_report_2014_k200; set c0051; run;
*/

data c0051; set con.FYFQ_analyst_report_2014_k200; run;
*지배주주순이익만 일단 남김;
/*data c006; set c0051; drop TP SALES OP NI EPS EPS_CONTROL; LENGTH CON_STATUS $10.; RUN;*/

*(수정)영업이익만 남김;
data c006; set c0051; drop TP SALES NI EPS NI_CONTROL EPS_CONTROL; LENGTH CON_STATUS $10.;
* 컨센서스 단순화 --> STRBUY, BUY, HOLD, REDUCE, SELL;

*consensus_status 변수 간소화;
/*
proc freq data=c006; table consensus_status; run;
proc freq data=c006; table GUBUN; run;
*/

*consensus_status 
BUY 173158 61.60 173158 61.60 
HOLD 19254 6.85 192412 68.45 
MARKETPERFORM 5410 1.92 197822 70.38 
NA 25 0.01 197847 70.38 
NEUTRAL 1139 0.41 198986 70.79 
NOT RATED 495 0.18 199481 70.97 
NR 253 0.09 199734 71.06 
NULL 2691 0.96 202425 72.01 
Not Rated 24 0.01 202449 72.02 
OUTPERFORM 641 0.23 203090 72.25 
REDUCE 212 0.08 203302 72.32 
SELL 104 0.04 203406 72.36 
STRONG BUY 1400 0.50 204806 72.86 
STRONGBUY 9 0.00 204815 72.86 
TRADING BUY 2274 0.81 207089 73.67 
UNDERPERFORM 156 0.06 207245 73.73 
강력매수 13 0.00 207258 73.73 
매수 67848 24.14 275106 97.87 
보유 342 0.12 275448 97.99 
비중축소 130 0.05 275578 98.04 
비중확대 1 0.00 275579 98.04 
시장수익률 278 0.10 275857 98.14 
시장수익률하회 8 0.00 275865 98.14 
시장평균 538 0.19 276403 98.33 
적극매수 1 0.00 276404 98.33 
중립 4692 1.67 281096 100.00 
;

if consensus_status = "BUY"  then con_status="BUY";
if consensus_status = "HOLD"  then con_status="HOLD";
if consensus_status = "MARKETPERFORM"  then con_status="HOLD";
if consensus_status = "NA"  then con_status="";
if consensus_status = "NEUTRAL"  then con_status="HOLD";
if consensus_status = "NOT"  then con_status="";
if consensus_status = "NR"  then con_status="";
if consensus_status = "NULL"  then con_status="";
if consensus_status = "Not"  then con_status="";
if consensus_status = "OUTPERFORM"  then con_status="HOLD";
if consensus_status = "REDUCE"  then con_status="REDUCE";
if consensus_status = "SELL"  then con_status="SELL";
if consensus_status = "STRONG"  then con_status="STRONGBUY";
if consensus_status = "STRONGBUY"  then con_status="STRONGBUY";
if consensus_status = "TRADING"  then con_status="HOLD";
if consensus_status = "UNDERPERFORM"  then con_status=" ";
if consensus_status = "강력매수"  then con_status="STRONGBUY";
if consensus_status = "매수"  then con_status="BUY";
if consensus_status = "보유"  then con_status="HOLD";
if consensus_status = "비중축소"  then con_status="REDUCE";
if consensus_status = "비중확대"  then con_status="BUY";
if consensus_status = "시장수익률"  then con_status="HOLD";
if consensus_status = "시장수익률하회"  then con_status="REDUCE";
if consensus_status = "시장평균"  then con_status="HOLD";
if consensus_status = "적극매수"  then con_status="STRONGBUY";
if consensus_status = "중립"  then con_status="HOLD";

IF OP NE .; 
run;


*회사별 컨센서스 수 계산;
PROC FREQ DATA=C006 NOPRINT; TABLE NAME / OUT=C006_FREQ;RUN;
proc sort data=C006_FREQ; by descending count; run;

data c0021; set c002; if year(date)>=2014; run;
PROC FREQ DATA=C0021 NOPRINT; TABLE NAME / OUT=C002_FREQ;RUN;
proc sort data=C002_FREQ; by descending count; run;






/******RESTART - FY, 연결재무제표 추정치만 남김 ******/

/*resetline; ods html close; ods graphics off; ods listing;*/

/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
*%let path=C:\Users\Administrator\Desktop\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";


data _FY_REPORTS_2011_2016;
    infile "&path.\FY_CONSENSUS_2011-2016.csv" dsd lrecl=99999 firstobs=2;
INFORMAT ID 7. date yymmdd10. symbol $8. name $30. gubun $20. report_issuer $20. analyst $12.
consensus_status $20. gyulsan 8. FY $6. TP 10. SALES 20. OP 20. NI 20. EPS BEST15. NI_CONTROL 20. EPS_CONTROL BEST15.;
INPUT ID date symbol name gubun report_issuer analyst consensus_status gyulsan FY TP SALES OP NI EPS NI_CONTROL EPS_CONTROL;
format date yymmdd10.;
RUN;


proc sort data=_FY_REPORTS_2011_2016; by name analyst gyulsan; run;

data con.FY_REPORTS_2011_2016; set _FY_REPORTS_2011_2016; run;

data a001; set con.FY_REPORTS_2011_2016; run; PROC FREQ DATA=A001; TABLE GUBUN; RUN;

/*===FY 예측치 데이터, 2014년 이후, KOSPI 50, IFRS연결재무제표 데이터만 남김===*/
*2014년 이후, kospi50으로 축소;
DATA _K50; SET CON.K50; drop MKTCAP; RUN; proc sort data=a001; by symbol; proc sort data=_k50; by symbol; run;
data a002; merge a001 _k50; by symbol; if RANK NE .; if year(date) >= 2014; run;
/*data a002; merge a001 _k50; by symbol; if RANK NE .; if year(date) >= 2014; run;*/

proc sort data=a002; by date; run; PROC FREQ DATA=A002; TABLE GUBUN; RUN;
*  gubun          빈도      백분율       빈도      백분율
IFRS-별도        3225      5.69         3225      5.69
IFRS-연결       53452     94.31        56677    100.00;

*IFRS-별도 부분 삭제, 지배주주순이익 컨센서스만 남김(결측값 제거), 컨센서스 status 간결하게 바꾸고 뒤로 뺌;
/*DATA A003; SET A002; IF GUBUN = "IFRS-연결"; drop TP SALES OP NI EPS EPS_CONTROL ; */
/*if NI_CONTROL NE .;*/

*IFRS-별도 부분 삭제, 영업이익 컨센서스만 남김(결측값 제거), 컨센서스 status 간결하게 바꾸고 뒤로 뺌;
DATA A003; SET A002; IF GUBUN = "IFRS-연결"; drop TP SALES NI EPS NI_CONTROL EPS_CONTROL ; 
if OP NE .;

length con_status $10.;
if consensus_status = "BUY"  then con_status="BUY";
if consensus_status = "HOLD"  then con_status="HOLD";
if consensus_status = "MARKETPERFORM"  then con_status="HOLD";
if consensus_status = "NA"  then con_status="";
if consensus_status = "NEUTRAL"  then con_status="HOLD";
if consensus_status = "NOT"  then con_status="";
if consensus_status = "NR"  then con_status="";
if consensus_status = "NULL"  then con_status="";
if consensus_status = "Not"  then con_status="";
if consensus_status = "OUTPERFORM"  then con_status="HOLD";
if consensus_status = "REDUCE"  then con_status="REDUCE";
if consensus_status = "SELL"  then con_status="SELL";
if consensus_status = "STRONG"  then con_status="STRONGBUY";
if consensus_status = "STRONGBUY"  then con_status="STRONGBUY";
if consensus_status = "TRADING"  then con_status="HOLD";
if consensus_status = "UNDERPERFORM"  then con_status=" ";
if consensus_status = "강력매수"  then con_status="STRONGBUY";
if consensus_status = "매수"  then con_status="BUY";
if consensus_status = "보유"  then con_status="HOLD";
if consensus_status = "비중축소"  then con_status="REDUCE";
if consensus_status = "비중확대"  then con_status="BUY";
if consensus_status = "시장수익률"  then con_status="HOLD";
if consensus_status = "시장수익률하회"  then con_status="REDUCE";
if consensus_status = "시장평균"  then con_status="HOLD";
if consensus_status = "적극매수"  then con_status="STRONGBUY";
if consensus_status = "중립"  then con_status="HOLD";
drop gubun consensus_status;
RUN; 
proc sort data=A003; by name analyst gyulsan date; run;

data a003_15; set a003; if gyulsan=201512; data a003_16; set a003; if gyulsan=201612; run;


*temp - 회사/증권사별 과거 컨센 찾아보기; 
DATA A015_TMP; SET a003_15; if year(date) < 2014 then delete; IF name="삼성전자" and report_issuer = "미래에셋증권"; RUN;
DATA A015_TMp3; SET a003_15; if year(date) < 2014 then delete; IF name="삼성전자" and report_issuer = "한국투자증권"; RUN;


*15E 컨센서스 해당기간: 2014.07~2015.06으로 조정; 
DATA A015; SET a003_15; 
if year(date) < 2014 then delete;
if year(date) = 2014 and month(date) le 6 then delete; 
if year(date) = 2015 and month(date) ge 7 then delete; run; 

*16E 컨센서스 해당기간: 2015.07~2016.06으로 조정; 
DATA A016; SET a003_16; 
if year(date) < 2015 then delete;
if year(date) = 2015 and month(date) le 6 then delete; 
if year(date) = 2016 and month(date) ge 7 then delete; run; 

proc freq data=a015 noprint; tables name*report_issuer / out=a015_freq;
proc freq data=a016 noprint; tables name*report_issuer / out=a016_freq;

proc sort data=a015_freq; by count; run;
proc sort data=a016_freq; by count; run;


/*  FILTER #1: 1년간 각 증권사에서 내놓은 실적 데이터 3개 미만인 종목/발행사 삭제
  (DEPRECATED)FILTER #2: 1년간 나온 총 실적 데이터가 100개 미만인 종목/발행사 삭제      */

DATA A015_Filter; SET a015_freq; if count ge 3; drop percent; run;
proc sort data=a015; by name report_issuer; 
proc sort data=a015_filter; by name report_issuer;  
data a_filtered_2015; merge a015 a015_filter; by name report_issuer; if count ne .; run;



DATA A016_Filter; SET a016_freq; if count ge 3; drop percent; run;
proc sort data=a016; by name report_issuer; 
proc sort data=a016_filter; by name report_issuer;  
data a_filtered_2016; merge a016 a016_filter; by name report_issuer; if count ne .; run;

PROC SORT DATA=

/*

data con.fy2015_analyst_oi_data; set a_filtered_2015; run;
data con.fy2016_analyst_oi_data; set a_filtered_2016; run;

*/

 
/* 낙관적 / 비관적 애널리스트 찾기 */






/******RESTART - 최종적으로 선정된 32종목에 대해 시그널 생성 ******/

/*resetline; ods html close; ods graphics off; ods listing;*/

/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
*%let path=C:\Users\Administrator\Desktop\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";


data _FY_REPORTS_2011_2016;
    infile "&path.\FY_CONSENSUS_2011-2016.csv" dsd lrecl=99999 firstobs=2;
INFORMAT ID 7. date yymmdd10. symbol $8. name $30. gubun $20. report_issuer $20. analyst $12.
consensus_status $20. gyulsan 8. FY $6. TP 10. SALES 20. OP 20. NI 20. EPS BEST15. NI_CONTROL 20. EPS_CONTROL BEST15.;
INPUT ID date symbol name gubun report_issuer analyst consensus_status gyulsan FY TP SALES OP NI EPS NI_CONTROL EPS_CONTROL;
format date yymmdd10.;
RUN;


proc sort data=_FY_REPORTS_2011_2016; by name analyst gyulsan; run;

data con.FY_REPORTS_2011_2016; set _FY_REPORTS_2011_2016; run;

data a001; set con.FY_REPORTS_2011_2016; run; 
PROC FREQ DATA=A001 NOPRINT; TABLE GYULSAN*GUBUN / OUT=A001_FREQ; RUN;
* 
GAAP-별도 22119 5.90 22119 5.90 
GAAP-연결 948 0.25 23067 6.16 
IFRS-별도 78925 21.07 101992 27.23 
IFRS-연결 272595 72.77 374587 100.00 
;
/* (DEPRECATED) */
/*FY 예측치 데이터, 2011년 이후 kospi32으로 축소 */
/*DATA _K32; SET CON.K32; KEEP SYMBOL NAME RANK; RANK=_N_; */
/*proc sort data=a001; by symbol; proc sort data=_k32; by symbol; run;*/
/**/
/*data a002; merge a001 _k32; by symbol; if RANK NE .; run;*/
/*=============*/

*SECTOR 정보 있는 192개 종목만; 
DATA _K192; SET CON._SECTOR; 
proc sort data=a001; by symbol; proc sort data=_K192; by symbol; run;

data a002; RETAIN symbol name sec_code sec_name report_issuer analyst gyulsan date;
merge a001 _K192; by symbol; if sec_code ne ""; run;


*data 걸러내기:
- 영업이익 컨센서스만 남김(결측값 제거), 
- IFRS-연결만 남김
- 2017년 예측치 제거
- 컨센서스 status 간결하게 바꾸고 뒤로 뺌;

DATA A003; SET A002; drop TP SALES NI EPS NI_CONTROL EPS_CONTROL ; 

if GUBUN = "IFRS-연결"; 
if OP NE .;
if gyulsan = 201712 then delete;
if gyulsan = 201812 then delete;
if gyulsan = 201912 then delete;
if gyulsan = 202012 then delete;

length con_status $10.;
if consensus_status = "BUY"  then con_status="BUY";
if consensus_status = "HOLD"  then con_status="HOLD";
if consensus_status = "MARKETPERFORM"  then con_status="HOLD";
if consensus_status = "NA"  then con_status="";
if consensus_status = "NEUTRAL"  then con_status="HOLD";
if consensus_status = "NOT"  then con_status="";
if consensus_status = "NR"  then con_status="";
if consensus_status = "NULL"  then con_status="";
if consensus_status = "Not"  then con_status="";
if consensus_status = "OUTPERFORM"  then con_status="HOLD";
if consensus_status = "REDUCE"  then con_status="REDUCE";
if consensus_status = "SELL"  then con_status="SELL";
if consensus_status = "STRONG"  then con_status="STRONGBUY";
if consensus_status = "STRONGBUY"  then con_status="STRONGBUY";
if consensus_status = "TRADING"  then con_status="HOLD";
if consensus_status = "UNDERPERFORM"  then con_status=" ";
if consensus_status = "강력매수"  then con_status="STRONGBUY";
if consensus_status = "매수"  then con_status="BUY";
if consensus_status = "보유"  then con_status="HOLD";
if consensus_status = "비중축소"  then con_status="REDUCE";
if consensus_status = "비중확대"  then con_status="BUY";
if consensus_status = "시장수익률"  then con_status="HOLD";
if consensus_status = "시장수익률하회"  then con_status="REDUCE";
if consensus_status = "시장평균"  then con_status="HOLD";
if consensus_status = "적극매수"  then con_status="STRONGBUY";
if consensus_status = "중립"  then con_status="HOLD";
drop consensus_status;
RUN; 
proc sort data=A003; by name analyst gyulsan date; run;

/*
*테스트용;
data a004; set a003; by name analyst gyulsan date; OP_LAGGED = lag(OP); 
if first.gyulsan then OP_LAGGED = .; run;

*Lagged 추정치와 현재 추정치의 차이 퍼센트로 비교;
data a0041; set a004; OP_DIFF = ( (OP - OP_LAGGED) / OP )* 100; 
*winsorize; op_diff_win = op_diff; 
if op_diff_win < -1 then op_diff_win = 1.01; 
if op_diff_win > 1 then op_diff_win = 1.01;
if op_diff_win = 0 then op_diff_win = 1.01; run;
proc univariate data=a0041; var op_diff_win; histogram op_diff_win / endpoints = -1 to 1.1 by .1; run;

data a0041_tmp; set a0041; if name = "KT" and analyst="김미송" and gyulsan = 201312; run;
*/

*OP추정치 상향/하향 기록 -  애널리스트/결산년도별로 구분
이전 추정치에 비해 상승시 +1, 하향시 -1, 동일할 시 이전 값 그대로
(+/- 0.1% 미만일 경우 이전 대비 동일한 추정치로 간주);

data a004; set a003; by name analyst gyulsan date; OP_LAGGED = lag(OP); 
if first.gyulsan then OP_LAGGED = .; OP_DIFF = ( (OP - OP_LAGGED) / OP )* 100; 
if abs(OP_DIFF) < 0.1 then OP_DIFF = 0; 
if op_lagged = . then op_diff = .;
run;

data a005; *LENGTH Symbol $8. NAME $30. Analyst $100.; set a004; by name analyst gyulsan date;
var = op_diff; lag_var = lag(var); retain OP_CHG;

*OP_DIFF = 0인 경우 이전의 추세값 그대로 유지;
if first.gyulsan then OP_CHG = .; 
else if (var = 0 and var < lag_var) or var > 0 then OP_CHG = 1;
else if (var = 0 and var > lag_var) or var < 0 then OP_CHG = -1;

*이전의 변화를 알 수 없고(OP_CHG값이 결측값) 변화량 또한(OP_DIFF) 0일 때 OP_CHG=0;
if lag(OP_CHG) = . AND OP_DIFF = 0 THEN OP_CHG = 0;

DROP VAR LAG_VAR; 

DROP ID GUBUN FY CON_STATUS; 

RUN;


proc sort data=a005; by name analyst gyulsan date; run;

%change(work.a005);
/* a005 - 컨센서스 데이터
*data con.a005; set a005; run;
data con.a005_SEC; set a005_; run;
*/




/*RESTART*/

/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";


/* 종가  */
data _k200_ap;
    infile "&path.\k200수정종가.csv" dsd lrecl=99999 firstobs=2;
  informat symbol $7. name $30. kind $15. item $12. item_name $100.  frequency $10. d1-d1692 best15.;
  input symbol name kind item item_name   frequency d1-d1692;
data _date; set con._date; 

/* day변수 col --> row 정렬 */
proc sort data=_k200_ap; by symbol name item item_name; run;
proc transpose data=_k200_ap out=_k200_tmp1; var d1-d1692; by symbol name item item_name; run;
*label 제거;  proc datasets library=work nolist;  modify _k200_tmp1;  attrib _all_ label=''; quit;
data _k200_tmp2; set _k200_tmp1; rename _name_=d; run;

/*day 변수 d1~d**** 에서 실제 날짜로 변환*/
proc sort data=_k200_tmp2; by d;
proc sort data=_date; by d;
data _k200_tmp3; retain symbol name date; merge _k200_tmp2 _date; by d; drop d item item_name; 
rename col1 = Adj_P; format date yymmdd10.;
proc sort data=_k200_tmp3; by symbol name date; run;

*data con._sector;
data _sector;
infile "&path.\k200업종.csv" dsd lrecl=99999 firstobs=2;
informat symbol $7. name $30. sec_code $7. sec_name $30.;
input symbol name sec_code sec_name; run;

proc sort data=_k200_tmp3; by symbol name; proc sort data=_sector; by symbol name;
data _k200_price; retain symbol name sec_code sec_name; merge _k200_tmp3 _sector; by symbol name; 
if adj_p ne .; 
run;
/*
data con._k200_price; set _k200_price; run;
*/


/* 종가  */
data _k200_m;
    infile "&path.\k200시가총액.csv" dsd lrecl=99999 firstobs=10;
  informat symbol $7. name $30. kind $15. item $12. item_name $100.  frequency $10. d1-d1692 best15.;
  input symbol name kind item item_name   frequency d1-d1692;
data _date; set con._date; 

/* day변수 col --> row 정렬 */
proc sort data=_k200_m; by symbol name item item_name; run;
proc transpose data=_k200_m out=_k200_tmp1; var d1-d1692; by symbol name item item_name; run;
*label 제거;  proc datasets library=work nolist;  modify _k200_tmp1;  attrib _all_ label=''; quit;
data _k200_tmp2; set _k200_tmp1; rename _name_=d; run;

/*day 변수 d1~d**** 에서 실제 날짜로 변환*/
proc sort data=_k200_tmp2; by d;
proc sort data=_date; by d;
data _k200_tmp3; retain symbol name date; merge _k200_tmp2 _date; by d; drop d item item_name; 
rename col1 = MKTCAP; label col1 = "시가총액(억원)"; format date yymmdd10.;
proc sort data=_k200_tmp3; by symbol name date; run;

/*
data con._k200_mktcap; set _k200_tmp3; run;
*/


/* ==================== 컨센서스 데이터 회사별로 분리 =====================*/

data a005; set con.a005_SEC; proc freq data=a005 noprint; table report_issuer / out=a005_issuers; 
data a005_issuers; retain n; set a005_issuers; n=_n_;
/*issuer1 ~ issuer50 변수에 증권사명 입력*/
data _null_; set a005_issuers; suffix=put(_n_,5.); call symput(cats('issuer',suffix), report_issuer); run; 
data _consensus; set a005; 
run;

/*결산년도별로 컨센서스 쪼개기*/
data A2013; set _consensus; if gyulsan = 201312;
data A2014; set _consensus; if gyulsan = 201412;
data A2015; set _consensus; if gyulsan = 201512;
data A2016; set _consensus; if gyulsan = 201612;
proc freq data=A2013 noprint; table report_issuer / out=B2013; 
proc freq data=A2014 noprint; table report_issuer / out=B2014; 
proc freq data=A2015 noprint; table report_issuer / out=B2015; 
proc freq data=A2016 noprint; table report_issuer / out=B2016; 
data _null_; set B2013; suffix=put(_n_,5.); call symput(cats('issuer2013_',suffix), report_issuer);  
data _null_; set B2014; suffix=put(_n_,5.); call symput(cats('issuer2014_',suffix), report_issuer);  
data _null_; set B2015; suffix=put(_n_,5.); call symput(cats('issuer2015_',suffix), report_issuer);  
data _null_; set B2016; suffix=put(_n_,5.); call symput(cats('issuer2016_',suffix), report_issuer);  
%put &issuer2013_1 &issuer2014_1 &issuer2015_1 &issuer2016_1
;
run;



*********컨센서스 증권사별로 불러와서 종가데이터와 합쳐서 
최종적으로 OP, OP_CHG에 대한 증권사별 DATA 합치는 매크로**********;

*(중요!!) 반복문 두번 돌려야됨 --> 연도별로 먼저 for문, 그 다음 증권사별로 for 문;

************************ 1. 연도별 DO LOOP ************************;

************************ 2. 증권사별 DO LOOP ************************;


options nonotes;

%macro aaa2013(n_issuers);

*증권사별 리포트(A2013 - 38개 ,A2014 - 41개,A2015 - 43개,A2016 - 44개);
%LET year = 2013;

*최초 가격 데이터셋(M0000) - 여기에 계속 칼럼 더해나감;
data m0000; set con._k200_price; 
*연도 구분하는 부분; 
if year(date) < &year. - 1 then delete; if year(date) > &year then delete; 
if year(date) = &year. - 1 and month(date) le 6 then delete; 
if year(date) = &year. and month(date) ge 7 then delete; 
proc sort data=m0000; by symbol name date; run;

data m_op_2013; set m0000; data m_op_chg_2013; set m0000; data m_op_diff_2013; set m0000; run;

*연도별 보고서 수(A2013 - 38개 ,A2014 - 41개,A2015 - 43개,A2016 - 44개);
%do i=1 %to &n_issuers; *38; %put &year &i &&issuer2013_&i;

data m0010; set A2013; if report_issuer = "&&issuer2013_&i"; 

*m0011 - OP변수; data m0011; set m0010; keep symbol name date op;
label op = "&&issuer2013_&i"; rename op = op_&i;

*m0012 - OP_CHG변수; data m0012; set m0010; keep symbol name date op_chg;  
label op_chg = "&&issuer2013_&i"; rename op_chg = op_chg_&i;

*m0013 - OP_DIFF변수; data m0013; set m0010; keep symbol name date op_diff;  
label op_diff = "&&issuer2013_&i"; rename op_diff = op_diff_&i;

proc sort data=m0011; by symbol name date;
proc sort data=m0012; by symbol name date;
proc sort data=m0013; by symbol name date;

data m_op_2013; merge m_op_2013 m0011; by symbol name date; if adj_p ne .;
data m_op_chg_2013; merge m_op_chg_2013 m0012; by symbol name date; if adj_p ne .;
data m_op_diff_2013; merge m_op_diff_2013 m0013; by symbol name date; if adj_p ne .;

run;

%end;
%mend; 


%macro aaa2014(n_issuers);
%LET year = 2014;

*최초 가격 데이터셋(M0000) - 여기에 계속 칼럼 더해나감;
data m0000; set con._k200_price; if year(date) < &year. - 1 then delete; if year(date) > &year then delete; 
if year(date) = &year. - 1 and month(date) le 6 then delete; if year(date) = &year. and month(date) ge 7 then delete; 
proc sort data=m0000; by symbol name date; run;

data m_op_2014; set m0000; data m_op_chg_2014; set m0000; data m_op_diff_2014; set m0000; run;

%do i=1 %to &n_issuers; *38; %put &year &i &&issuer2014_&i;

data m0010; set A2014; if report_issuer = "&&issuer2014_&i"; 
*m0011 - OP변수; data m0011; set m0010; keep symbol name date op;
label op = "&&issuer2014_&i"; rename op = op_&i;
*m0012 - OP_CHG변수; data m0012; set m0010; keep symbol name date op_chg;  
label op_chg = "&&issuer2014_&i"; rename op_chg = op_chg_&i;
*m0013 - OP_DIFF변수; data m0013; set m0010; keep symbol name date op_diff;  
label op_diff = "&&issuer2014_&i"; rename op_diff = op_diff_&i;

proc sort data=m0011; by symbol name date; proc sort data=m0012; by symbol name date;
proc sort data=m0013; by symbol name date;

data m_op_2014; merge m_op_2014 m0011; by symbol name date; if adj_p ne .;
data m_op_chg_2014; merge m_op_chg_2014 m0012; by symbol name date; if adj_p ne .;
data m_op_diff_2014; merge m_op_diff_2014 m0013; by symbol name date; if adj_p ne .;

run;

%end;
%mend; 


%macro aaa2015(n_issuers);
%LET year = 2015;

*최초 가격 데이터셋(M0000) - 여기에 계속 칼럼 더해나감;
data m0000; set con._k200_price; if year(date) < &year. - 1 then delete; if year(date) > &year then delete; 
if year(date) = &year. - 1 and month(date) le 6 then delete; if year(date) = &year. and month(date) ge 7 then delete; 
proc sort data=m0000; by symbol name date; run;

data m_op_2015; set m0000; data m_op_chg_2015; set m0000; data m_op_diff_2015; set m0000; run;

%do i=1 %to &n_issuers; *38; %put &year &i &&issuer2015_&i;

data m0010; set A2015; if report_issuer = "&&issuer2015_&i"; 
*m0011 - OP변수; data m0011; set m0010; keep symbol name date op;
label op = "&&issuer2015_&i"; rename op = op_&i;
*m0012 - OP_CHG변수; data m0012; set m0010; keep symbol name date op_chg;  
label op_chg = "&&issuer2015_&i"; rename op_chg = op_chg_&i;
*m0013 - OP_DIFF변수; data m0013; set m0010; keep symbol name date op_diff;  
label op_diff = "&&issuer2015_&i"; rename op_diff = op_diff_&i;

proc sort data=m0011; by symbol name date; proc sort data=m0012; by symbol name date;
proc sort data=m0013; by symbol name date;

data m_op_2015; merge m_op_2015 m0011; by symbol name date; if adj_p ne .;
data m_op_chg_2015; merge m_op_chg_2015 m0012; by symbol name date; if adj_p ne .;
data m_op_diff_2015; merge m_op_diff_2015 m0013; by symbol name date; if adj_p ne .;

run;

%end;
%mend; 

%macro aaa2016(n_issuers);
%LET year = 2016;

*최초 가격 데이터셋(M0000) - 여기에 계속 칼럼 더해나감;
data m0000; set con._k200_price; if year(date) < &year. - 1 then delete; if year(date) > &year then delete; 
if year(date) = &year. - 1 and month(date) le 6 then delete; if year(date) = &year. and month(date) ge 7 then delete; 
proc sort data=m0000; by symbol name date; run;

data m_op_2016; set m0000; data m_op_chg_2016; set m0000; data m_op_diff_2016; set m0000; run;

%do i=1 %to &n_issuers; *38; %put &year &i &&issuer2016_&i;

data m0010; set A2016; if report_issuer = "&&issuer2016_&i"; 
*m0011 - OP변수; data m0011; set m0010; keep symbol name date op;
label op = "&&issuer2016_&i"; rename op = op_&i;
*m0012 - OP_CHG변수; data m0012; set m0010; keep symbol name date op_chg;  
label op_chg = "&&issuer2016_&i"; rename op_chg = op_chg_&i;
*m0013 - OP_DIFF변수; data m0013; set m0010; keep symbol name date op_diff;  
label op_diff = "&&issuer2016_&i"; rename op_diff = op_diff_&i;

proc sort data=m0011; by symbol name date; proc sort data=m0012; by symbol name date;
proc sort data=m0013; by symbol name date;

data m_op_2016; merge m_op_2016 m0011; by symbol name date; if adj_p ne .;
data m_op_chg_2016; merge m_op_chg_2016 m0012; by symbol name date; if adj_p ne .;
data m_op_diff_2016; merge m_op_diff_2016 m0013; by symbol name date; if adj_p ne .;

run;

%end;
%mend; 

*연도별 보고서 수(2013 - 38개 , 2014 - 41개, 2015 - 43개, 2016 - 44개);
%aaa2013(38);
%aaa2014(41);
%aaa2015(43);
%aaa2016(44);
options notes;


*증권사별 변수명 통일;

data _op_2013; SET m_op_2013;
  RENAME OP_1 = BS OP_2 = HMC OP_3 = IBK OP_4 = KB OP_5 = KDB
OP_6 = KTB OP_7 = LIG OP_8 = NH OP_9 = SK OP_10 = GBRDGE
OP_11 = KYOBO OP_12 = DAISHIN OP_13 = DONGBU OP_14 = DONGYANG OP_15 = LEADING
OP_16 = MERITZ OP_17 = MIRAE OP_18 = BARO OP_19 = BOOGUK OP_20 = SAMSUNG
OP_21 = SHINYOUNG OP_22 = SHINHAN OP_23 = IAM OP_24 = WOORI OP_25 = YUJIN
OP_26 = YUWHA OP_27 = ETRADE OP_28 = KIWOOM OP_29 = TAURUS OP_30 = HANA
OP_31 = HI OP_32 = HANKOOK OP_33 = HANMAK OP_34 = HANYANG OP_35 = HANWHA
OP_36 = HANWHATOOJA OP_37 = HYUNDAI OP_38 = HEUNGGUK;
data _op_chg_2013; SET m_op_chg_2013;
  RENAME OP_CHG_1 = BS OP_CHG_2 = HMC OP_CHG_3 = IBK OP_CHG_4 = KB OP_CHG_5 = KDB
OP_CHG_6 = KTB OP_CHG_7 = LIG OP_CHG_8 = NH OP_CHG_9 = SK OP_CHG_10 = GBRDGE
OP_CHG_11 = KYOBO OP_CHG_12 = DAISHIN OP_CHG_13 = DONGBU OP_CHG_14 = DONGYANG OP_CHG_15 = LEADING
OP_CHG_16 = MERITZ OP_CHG_17 = MIRAE OP_CHG_18 = BARO OP_CHG_19 = BOOGUK OP_CHG_20 = SAMSUNG
OP_CHG_21 = SHINYOUNG OP_CHG_22 = SHINHAN OP_CHG_23 = IAM OP_CHG_24 = WOORI OP_CHG_25 = YUJIN
OP_CHG_26 = YUWHA OP_CHG_27 = ETRADE OP_CHG_28 = KIWOOM OP_CHG_29 = TAURUS OP_CHG_30 = HANA
OP_CHG_31 = HI OP_CHG_32 = HANKOOK OP_CHG_33 = HANMAK OP_CHG_34 = HANYANG OP_CHG_35 = HANWHA
OP_CHG_36 = HANWHATOOJA OP_CHG_37 = HYUNDAI OP_CHG_38 = HEUNGGUK;
data _op_diff_2013; SET m_op_diff_2013;
  RENAME OP_DIFF_1 = BS OP_DIFF_2 = HMC OP_DIFF_3 = IBK OP_DIFF_4 = KB OP_DIFF_5 = KDB
OP_DIFF_6 = KTB OP_DIFF_7 = LIG OP_DIFF_8 = NH OP_DIFF_9 = SK OP_DIFF_10 = GBRDGE
OP_DIFF_11 = KYOBO OP_DIFF_12 = DAISHIN OP_DIFF_13 = DONGBU OP_DIFF_14 = DONGYANG OP_DIFF_15 = LEADING
OP_DIFF_16 = MERITZ OP_DIFF_17 = MIRAE OP_DIFF_18 = BARO OP_DIFF_19 = BOOGUK OP_DIFF_20 = SAMSUNG
OP_DIFF_21 = SHINYOUNG OP_DIFF_22 = SHINHAN OP_DIFF_23 = IAM OP_DIFF_24 = WOORI OP_DIFF_25 = YUJIN
OP_DIFF_26 = YUWHA OP_DIFF_27 = ETRADE OP_DIFF_28 = KIWOOM OP_DIFF_29 = TAURUS OP_DIFF_30 = HANA
OP_DIFF_31 = HI OP_DIFF_32 = HANKOOK OP_DIFF_33 = HANMAK OP_DIFF_34 = HANYANG OP_DIFF_35 = HANWHA
OP_DIFF_36 = HANWHATOOJA OP_DIFF_37 = HYUNDAI OP_DIFF_38 = HEUNGGUK;

data _op_2014; SET m_op_2014;
  RENAME OP_1 = BS OP_2 = HMC OP_3 = IBK OP_4 = KB OP_5 = KDB 
OP_6 = KTB OP_7 = LIG OP_8 = NH OP_9 = NHTOOJA OP_10 = SK 
OP_11 = NONGHYUP OP_12 = GBRDGE OP_13 = KYOBO OP_14 = DAISHIN OP_15 = DONGBU
OP_16 = DONGYANG OP_17 = LEADING OP_18 = MERITZ OP_19 = MIRAE OP_20 = BARO
OP_21 = BOOGUK OP_22 = SAMSUNG OP_23 = SHINYOUNG OP_24 = SHINHAN OP_25 = IAM
OP_26 = WOORI OP_27 = YUANTA OP_28 = YUJIN OP_29 = YUWHA OP_30 = ETRADE
OP_31 = KIWOOM OP_32 = TAURUS OP_33 = HANA OP_34 = HI OP_35 = HANKOOK
OP_36 = HANMAK OP_37 = HANYANG OP_38 = HANWHA OP_39 = HANWHATOOJA OP_40 = HYUNDAI
OP_41 = HEUNGGUK;
data _op_chg_2014; SET m_op_chg_2014;
  RENAME OP_CHG_1 = BS OP_CHG_2 = HMC OP_CHG_3 = IBK OP_CHG_4 = KB OP_CHG_5 = KDB 
OP_CHG_6 = KTB OP_CHG_7 = LIG OP_CHG_8 = NH OP_CHG_9 = NHTOOJA OP_CHG_10 = SK 
OP_CHG_11 = NONGHYUP OP_CHG_12 = GBRDGE OP_CHG_13 = KYOBO OP_CHG_14 = DAISHIN OP_CHG_15 = DONGBU
OP_CHG_16 = DONGYANG OP_CHG_17 = LEADING OP_CHG_18 = MERITZ OP_CHG_19 = MIRAE OP_CHG_20 = BARO
OP_CHG_21 = BOOGUK OP_CHG_22 = SAMSUNG OP_CHG_23 = SHINYOUNG OP_CHG_24 = SHINHAN OP_CHG_25 = IAM
OP_CHG_26 = WOORI OP_CHG_27 = YUANTA OP_CHG_28 = YUJIN OP_CHG_29 = YUWHA OP_CHG_30 = ETRADE
OP_CHG_31 = KIWOOM OP_CHG_32 = TAURUS OP_CHG_33 = HANA OP_CHG_34 = HI OP_CHG_35 = HANKOOK
OP_CHG_36 = HANMAK OP_CHG_37 = HANYANG OP_CHG_38 = HANWHA OP_CHG_39 = HANWHATOOJA OP_CHG_40 = HYUNDAI
OP_CHG_41 = HEUNGGUK;
data _op_diff_2014; SET m_op_diff_2014;
  RENAME OP_DIFF_1 = BS OP_DIFF_2 = HMC OP_DIFF_3 = IBK OP_DIFF_4 = KB OP_DIFF_5 = KDB 
OP_DIFF_6 = KTB OP_DIFF_7 = LIG OP_DIFF_8 = NH OP_DIFF_9 = NHTOOJA OP_DIFF_10 = SK 
OP_DIFF_11 = NONGHYUP OP_DIFF_12 = GBRDGE OP_DIFF_13 = KYOBO OP_DIFF_14 = DAISHIN OP_DIFF_15 = DONGBU
OP_DIFF_16 = DONGYANG OP_DIFF_17 = LEADING OP_DIFF_18 = MERITZ OP_DIFF_19 = MIRAE OP_DIFF_20 = BARO
OP_DIFF_21 = BOOGUK OP_DIFF_22 = SAMSUNG OP_DIFF_23 = SHINYOUNG OP_DIFF_24 = SHINHAN OP_DIFF_25 = IAM
OP_DIFF_26 = WOORI OP_DIFF_27 = YUANTA OP_DIFF_28 = YUJIN OP_DIFF_29 = YUWHA OP_DIFF_30 = ETRADE
OP_DIFF_31 = KIWOOM OP_DIFF_32 = TAURUS OP_DIFF_33 = HANA OP_DIFF_34 = HI OP_DIFF_35 = HANKOOK
OP_DIFF_36 = HANMAK OP_DIFF_37 = HANYANG OP_DIFF_38 = HANWHA OP_DIFF_39 = HANWHATOOJA OP_DIFF_40 = HYUNDAI
OP_DIFF_41 = HEUNGGUK;

data _op_2015; SET m_op_2015;
  RENAME OP_1 = BNK OP_2 = BS OP_3 = HMC OP_4 = IBK OP_5 = KB
OP_6 = KDB OP_7 = KTB OP_8 = LIG OP_9 = NH OP_10 = NHTOOJA
OP_11 = SK OP_12 = NONGHYUP OP_13 = KYOBO OP_14 = DAISHIN OP_15 = DONGBU
OP_16 = DONGYANG OP_17 = LEADING OP_18 = MERITZ OP_19 = MIRAE OP_20 = BARO
OP_21 = BOOGUK OP_22 = SAMSUNG OP_23 = SHINYOUNG OP_24 = SHINHAN OP_25 = IAM
OP_26 = WOORI OP_27 = YUANTA OP_28 = YUJIN OP_29 = YUWHA OP_30 = EBEST
OP_31 = ETRADE OP_32 = KORASSET OP_33 = KIWOOM OP_34 = TAURUS OP_35 = HANAGEUMTOO
OP_36 = HANA OP_37 = HI OP_38 = HANKOOK OP_39 = HANMAK OP_40 = HANYANG
OP_41 = HANWHATOOJA OP_42 = HYUNDAI OP_43 = HEUNGGUK;

data _op_chg_2015; SET m_op_chg_2015;
  RENAME OP_CHG_1 = BNK OP_CHG_2 = BS OP_CHG_3 = HMC OP_CHG_4 = IBK OP_CHG_5 = KB
OP_CHG_6 = KDB OP_CHG_7 = KTB OP_CHG_8 = LIG OP_CHG_9 = NH OP_CHG_10 = NHTOOJA
OP_CHG_11 = SK OP_CHG_12 = NONGHYUP OP_CHG_13 = KYOBO OP_CHG_14 = DAISHIN OP_CHG_15 = DONGBU
OP_CHG_16 = DONGYANG OP_CHG_17 = LEADING OP_CHG_18 = MERITZ OP_CHG_19 = MIRAE OP_CHG_20 = BARO
OP_CHG_21 = BOOGUK OP_CHG_22 = SAMSUNG OP_CHG_23 = SHINYOUNG OP_CHG_24 = SHINHAN OP_CHG_25 = IAM
OP_CHG_26 = WOORI OP_CHG_27 = YUANTA OP_CHG_28 = YUJIN OP_CHG_29 = YUWHA OP_CHG_30 = EBEST
OP_CHG_31 = ETRADE OP_CHG_32 = KORASSET OP_CHG_33 = KIWOOM OP_CHG_34 = TAURUS OP_CHG_35 = HANAGEUMTOO
OP_CHG_36 = HANA OP_CHG_37 = HI OP_CHG_38 = HANKOOK OP_CHG_39 = HANMAK OP_CHG_40 = HANYANG
OP_CHG_41 = HANWHATOOJA OP_CHG_42 = HYUNDAI OP_CHG_43 = HEUNGGUK;

data _op_diff_2015; SET m_op_diff_2015;
  RENAME OP_DIFF_1 = BNK OP_DIFF_2 = BS OP_DIFF_3 = HMC OP_DIFF_4 = IBK OP_DIFF_5 = KB
OP_DIFF_6 = KDB OP_DIFF_7 = KTB OP_DIFF_8 = LIG OP_DIFF_9 = NH OP_DIFF_10 = NHTOOJA
OP_DIFF_11 = SK OP_DIFF_12 = NONGHYUP OP_DIFF_13 = KYOBO OP_DIFF_14 = DAISHIN OP_DIFF_15 = DONGBU
OP_DIFF_16 = DONGYANG OP_DIFF_17 = LEADING OP_DIFF_18 = MERITZ OP_DIFF_19 = MIRAE OP_DIFF_20 = BARO
OP_DIFF_21 = BOOGUK OP_DIFF_22 = SAMSUNG OP_DIFF_23 = SHINYOUNG OP_DIFF_24 = SHINHAN OP_DIFF_25 = IAM
OP_DIFF_26 = WOORI OP_DIFF_27 = YUANTA OP_DIFF_28 = YUJIN OP_DIFF_29 = YUWHA OP_DIFF_30 = EBEST
OP_DIFF_31 = ETRADE OP_DIFF_32 = KORASSET OP_DIFF_33 = KIWOOM OP_DIFF_34 = TAURUS OP_DIFF_35 = HANAGEUMTOO
OP_DIFF_36 = HANA OP_DIFF_37 = HI OP_DIFF_38 = HANKOOK OP_DIFF_39 = HANMAK OP_DIFF_40 = HANYANG
OP_DIFF_41 = HANWHATOOJA OP_DIFF_42 = HYUNDAI OP_DIFF_43 = HEUNGGUK;


data _op_2016; SET m_op_2016;
  RENAME OP_1 = BNK OP_2 = BS OP_3 = HMC OP_4 = IBK OP_5 = KB
OP_6 = KDB OP_7 = KTB OP_8 = LIG OP_9 = NH OP_10 = NHTOOJA
OP_11 = SK OP_12 = NONGHYUP OP_13 = GBRDGE OP_14 = KYOBO OP_15 = DAISHIN
OP_16 = DONGBU OP_17 = DONGYANG OP_18 = LEADING OP_19 = MERITZ OP_20 = MIREADAEWOO
OP_21 = MIREAD OP_22 = MIRAE OP_23 = BARO OP_24 = BOOGUK OP_25 = SAMSUNG
OP_26 = SHINYOUNG OP_27 = SHINHAN OP_28 = IAM OP_29 = WOORI OP_30 = YUANTA
OP_31 = YUJIN OP_32 = YUWHA OP_33 = EBEST OP_34 = ETRADE OP_35 = KIWOOM
OP_36 = TAURUS OP_37 = HANAGEUMTOO OP_38 = HANA OP_39 = HI OP_40 = HANKOOK
OP_41 = HANYANG OP_42 = HANWHATOOJA OP_43 = HYUNDAI OP_44 = HEUNGGUK;

data _op_chg_2016; SET m_op_chg_2016;
  RENAME OP_CHG_1 = BNK OP_CHG_2 = BS OP_CHG_3 = HMC OP_CHG_4 = IBK OP_CHG_5 = KB
OP_CHG_6 = KDB OP_CHG_7 = KTB OP_CHG_8 = LIG OP_CHG_9 = NH OP_CHG_10 = NHTOOJA
OP_CHG_11 = SK OP_CHG_12 = NONGHYUP OP_CHG_13 = GBRDGE OP_CHG_14 = KYOBO OP_CHG_15 = DAISHIN
OP_CHG_16 = DONGBU OP_CHG_17 = DONGYANG OP_CHG_18 = LEADING OP_CHG_19 = MERITZ OP_CHG_20 = MIREADAEWOO
OP_CHG_21 = MIREAD OP_CHG_22 = MIRAE OP_CHG_23 = BARO OP_CHG_24 = BOOGUK OP_CHG_25 = SAMSUNG
OP_CHG_26 = SHINYOUNG OP_CHG_27 = SHINHAN OP_CHG_28 = IAM OP_CHG_29 = WOORI OP_CHG_30 = YUANTA
OP_CHG_31 = YUJIN OP_CHG_32 = YUWHA OP_CHG_33 = EBEST OP_CHG_34 = ETRADE OP_CHG_35 = KIWOOM
OP_CHG_36 = TAURUS OP_CHG_37 = HANAGEUMTOO OP_CHG_38 = HANA OP_CHG_39 = HI OP_CHG_40 = HANKOOK
OP_CHG_41 = HANYANG OP_CHG_42 = HANWHATOOJA OP_CHG_43 = HYUNDAI OP_CHG_44 = HEUNGGUK;

data _op_diff_2016; SET m_op_diff_2016;
  RENAME OP_DIFF_1 = BNK OP_DIFF_2 = BS OP_DIFF_3 = HMC OP_DIFF_4 = IBK OP_DIFF_5 = KB
OP_DIFF_6 = KDB OP_DIFF_7 = KTB OP_DIFF_8 = LIG OP_DIFF_9 = NH OP_DIFF_10 = NHTOOJA
OP_DIFF_11 = SK OP_DIFF_12 = NONGHYUP OP_DIFF_13 = GBRDGE OP_DIFF_14 = KYOBO OP_DIFF_15 = DAISHIN
OP_DIFF_16 = DONGBU OP_DIFF_17 = DONGYANG OP_DIFF_18 = LEADING OP_DIFF_19 = MERITZ OP_DIFF_20 = MIREADAEWOO
OP_DIFF_21 = MIREAD OP_DIFF_22 = MIRAE OP_DIFF_23 = BARO OP_DIFF_24 = BOOGUK OP_DIFF_25 = SAMSUNG
OP_DIFF_26 = SHINYOUNG OP_DIFF_27 = SHINHAN OP_DIFF_28 = IAM OP_DIFF_29 = WOORI OP_DIFF_30 = YUANTA
OP_DIFF_31 = YUJIN OP_DIFF_32 = YUWHA OP_DIFF_33 = EBEST OP_DIFF_34 = ETRADE OP_DIFF_35 = KIWOOM
OP_DIFF_36 = TAURUS OP_DIFF_37 = HANAGEUMTOO OP_DIFF_38 = HANA OP_DIFF_39 = HI OP_DIFF_40 = HANKOOK
OP_DIFF_41 = HANYANG OP_DIFF_42 = HANWHATOOJA OP_DIFF_43 = HYUNDAI OP_DIFF_44 = HEUNGGUK;
RUN;

*매크로 결과 나온 데이터셋 합치기;

data __OP; set _op_2013 _op_2014 _op_2015 _op_2016; 
proc sort data=__OP; by symbol; proc sort data=con.k200_rank; by symbol;
data __OP2; merge __OP con.k200_rank; by symbol;  
data __OP2; retain symbol name no; set __OP2; 

data __OP_CHG; set _op_CHG_2013 _op_CHG_2014 _op_CHG_2015 _op_CHG_2016; 
proc sort data=__OP_CHG; by symbol; proc sort data=con.k200_rank; by symbol;
data __OP_CHG2; merge __OP_CHG con.k200_rank; by symbol;  
data __OP_CHG2; retain symbol name no; set __OP_CHG2; 

data __OP_DIFF; set _op_DIFF_2013 _op_DIFF_2014 _op_DIFF_2015 _op_DIFF_2016; 
proc sort data=__OP_DIFF; by symbol; proc sort data=con.k200_rank; by symbol;
data __OP_DIFF2; merge __OP_DIFF con.k200_rank; by symbol;  
data __OP_DIFF2; retain symbol name no; set __OP_DIFF2; run;


PROC SORT DATA=__OP2; BY NAME DATE; 
PROC SORT DATA=__OP_CHG2; BY NAME DATE; 
PROC SORT DATA=__OP_DIFF2; BY NAME DATE; 

RUN;

/* _op: 애널리스트의 영업이익
   _op_chg: 영업이익 변화추세(+1,0,-1)
   _op_diff: 영업이익 증감

각각 저장

data con._op; set __op2; data con._op_chg; set __op_chg2; data con._op_diff; set __op_diff2; run;
*/

/*******RESTART******/

*매매신호 포착;
*종목별 index 매김;
/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
*%let path=U:\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";

data c001; set con._op; 
data c002; set con._op_chg; 
data c003; set con._op_diff; run;

*Analyst Update Ratio 계산;
data c0021; set c002; con_sum = sum(BARO, BNK, BOOGUK, BS, DAISHIN, DONGBU, DONGYANG, EBEST, ETRADE, GBRDGE, 
HANA, HANAGEUMTOO, HANKOOK, HANMAK, HANWHA, HANWHATOOJA, HANYANG, 
HEUNGGUK, HI, HMC, HYUNDAI, IAM, IBK, KB, KDB, KIWOOM, KORASSET, KTB, KYOBO, 
LEADING, LIG, MERITZ, MIRAE, MIREAD, MIREADAEWOO, NH, NHTOOJA, NONGHYUP, 
SAMSUNG, SHINHAN, SHINYOUNG, SK, TAURUS, WOORI, YUANTA, YUJIN, YUWHA); 

con_count = sum(ABS(BARO), ABS(BNK), ABS(BOOGUK), ABS(BS), ABS(DAISHIN), ABS(DONGBU), ABS(DONGYANG), 
ABS(EBEST), ABS(ETRADE), ABS(GBRDGE), ABS(HANA), ABS(HANAGEUMTOO), ABS(HANKOOK), 
ABS(HANMAK), ABS(HANWHA), ABS(HANWHATOOJA), ABS(HANYANG), ABS(HEUNGGUK), 
ABS(HI), ABS(HMC), ABS(HYUNDAI), ABS(IAM), ABS(IBK), ABS(KB), ABS(KDB), ABS(KIWOOM), 
ABS(KORASSET), ABS(KTB), ABS(KYOBO), ABS(LEADING), ABS(LIG), ABS(MERITZ), ABS(MIRAE), 
ABS(MIREAD), ABS(MIREADAEWOO), ABS(NH), ABS(NHTOOJA), ABS(NONGHYUP), ABS(SAMSUNG), 
ABS(SHINHAN), ABS(SHINYOUNG), ABS(SK), ABS(TAURUS), ABS(WOORI), ABS(YUANTA), ABS(YUJIN), ABS(YUWHA));

AUR_1day = con_sum / con_count; 

run;

proc sort data=c0021; by symbol date; run;

data c0022; set c0021; by symbol date; retain nday 1; nday + 1;
if first.symbol then nday = 1; 
data c0022; retain symbol name no sec_code sec_name date nday con_sum con_count aur_1day; set c0022; 
rename BS = inst1 HMC = inst2 IBK = inst3 KB = inst4 KDB = inst5 KTB = inst6 LIG = inst7 NH = inst8
SK = inst9 GBRDGE = inst10 KYOBO = inst11 DAISHIN = inst12 DONGBU = inst13 
DONGYANG = inst14 LEADING = inst15 MERITZ = inst16 MIRAE = inst17 BARO = inst18
BOOGUK = inst19 SAMSUNG = inst20 SHINYOUNG = inst21 SHINHAN = inst22 IAM = inst23 WOORI = inst24
YUJIN = inst25 YUWHA = inst26 ETRADE = inst27 KIWOOM = inst28 TAURUS = inst29 HANA = inst30
HI = inst31 HANKOOK = inst32 HANMAK = inst33 HANYANG = inst34 HANWHA = inst35 HANWHATOOJA = inst36
HYUNDAI = inst37 HEUNGGUK = inst38 NHTOOJA = inst39 NONGHYUP = inst40 YUANTA = inst41
BNK = inst42 EBEST = inst43 KORASSET = inst44 HANAGEUMTOO = inst45 MIREADAEWOO = inst46
MIREAD = inst47; run;

/*data _tmp; set c0021; if _n_=1; run;
data m001; set c0022; if (name = "삼성전자" or name = "현대차" or 
name ="LG디스플레이" or name = "SK텔레콤")  and 1 <= nday <= 60; run;*/

data m0011; set c0022; if 1 <= nday <= 60; DROP con_sum con_count aur_1day; 
data m0012; set c0022; if 31 <= nday <= 60; DROP con_sum con_count aur_1day; run;


*최근 60일의 애널리스트 긍정/부정 추세;
data m0021; 
do _n_ = 1 by 1 until(last.symbol); 
update m0011(obs=0) m0011; by symbol;  end; 
do _n_ = 1 to _n_; output ; end ; 

*최근 30일의 애널리스트 긍정/부정 추세;
data m0022; 
do _n_ = 1 by 1 until(last.symbol); 
update m0012(obs=0) m0012; by symbol; end; 
do _n_ = 1 to _n_; output ; end ; 
run;

* - 60일동안의 추세 1줄로 요약

*update 전;
data m0031; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count;
set m0011;by symbol; if last.symbol; con_sum =sum(of inst1-inst47); con_count = sumabs(of inst1-inst47); 
*update 후;
data m003_60d; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count;
set m0021;by symbol; if last.symbol; con_sum =sum(of inst1-inst47); con_count = sumabs(of inst1-inst47); run;
data m003_30d; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count;
set m0022;by symbol; if last.symbol; con_sum =sum(of inst1-inst47); con_count = sumabs(of inst1-inst47); run;

 *- 1~1461일 동안 M002, M0021 만들어서 매일매일의 추세 기록;




/*******RESTART* AUR 일별데이터 만드는 매크로******/

/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
*%let path=U:\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";

data d001c; set con._op_chg; 
data d001d; set con._op_diff; run;

proc sort data=d001c; by symbol date; proc sort data=d001d; by symbol date; run;

*nday(종목별 데이터 수집날짜) 변수 추가;
data d002c; set d001c; by symbol date; retain nday 1; nday + 1;
if first.symbol then nday = 1; 
data d002d; set d001d; by symbol date; retain nday 1; nday + 1;
if first.symbol then nday = 1; 
*변수명 inst1 - inst47(칼럼 순서대로)로 변경;
data d002c; retain symbol name no sec_code sec_name date nday; set d002c; 
rename BS = inst1 HMC = inst2 IBK = inst3 KB = inst4 KDB = inst5 KTB = inst6 LIG = inst7 NH = inst8
SK = inst9 GBRDGE = inst10 KYOBO = inst11 DAISHIN = inst12 DONGBU = inst13 
DONGYANG = inst14 LEADING = inst15 MERITZ = inst16 MIRAE = inst17 BARO = inst18
BOOGUK = inst19 SAMSUNG = inst20 SHINYOUNG = inst21 SHINHAN = inst22 IAM = inst23 WOORI = inst24
YUJIN = inst25 YUWHA = inst26 ETRADE = inst27 KIWOOM = inst28 TAURUS = inst29 HANA = inst30
HI = inst31 HANKOOK = inst32 HANMAK = inst33 HANYANG = inst34 HANWHA = inst35 HANWHATOOJA = inst36
HYUNDAI = inst37 HEUNGGUK = inst38 NHTOOJA = inst39 NONGHYUP = inst40 YUANTA = inst41
BNK = inst42 EBEST = inst43 KORASSET = inst44 HANAGEUMTOO = inst45 MIREADAEWOO = inst46
MIREAD = inst47;
data d002d; retain symbol name no sec_code sec_name date nday; set d002d; 
rename BS = inst1 HMC = inst2 IBK = inst3 KB = inst4 KDB = inst5 KTB = inst6 LIG = inst7 NH = inst8
SK = inst9 GBRDGE = inst10 KYOBO = inst11 DAISHIN = inst12 DONGBU = inst13 
DONGYANG = inst14 LEADING = inst15 MERITZ = inst16 MIRAE = inst17 BARO = inst18
BOOGUK = inst19 SAMSUNG = inst20 SHINYOUNG = inst21 SHINHAN = inst22 IAM = inst23 WOORI = inst24
YUJIN = inst25 YUWHA = inst26 ETRADE = inst27 KIWOOM = inst28 TAURUS = inst29 HANA = inst30
HI = inst31 HANKOOK = inst32 HANMAK = inst33 HANYANG = inst34 HANWHA = inst35 HANWHATOOJA = inst36
HYUNDAI = inst37 HEUNGGUK = inst38 NHTOOJA = inst39 NONGHYUP = inst40 YUANTA = inst41
BNK = inst42 EBEST = inst43 KORASSET = inst44 HANAGEUMTOO = inst45 MIREADAEWOO = inst46
MIREAD = inst47; run;


/*data con._op_chg2; set d002c; 
data con._op_diff2; set  d002d; run;*/

/*데이터셋 초기화*/

data _aur_60day; set _null_;
data _aur_30day; set _null_; run;

/*매크로 시작 - nday = 60 to 1461*/

data m0011; set d002c; if 60 - 59 <= nday <= 60; 
data m0012; set d002c; if 60 - 29 <= nday <= 60; run;

*최근 60일의 애널리스트 긍정/부정 추세;
data m0021; do _n_ = 1 by 1 until(last.symbol); 
update m0011(obs=0) m0011; by symbol;  end; 
do _n_ = 1 to _n_; output ; end ; 
*최근 30일의 애널리스트 긍정/부정 추세;
data m0022; do _n_ = 1 by 1 until(last.symbol); 
update m0012(obs=0) m0012; by symbol; end; 
do _n_ = 1 to _n_; output ; end ; run;

* 30,60일동안의 추세 1줄로 요약;
data m003_60d; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count AUR;
set m0021;by symbol; if last.symbol; con_sum =sum(of inst1-inst47); con_count = sumabs(of inst1-inst47); 
if con_sum ne 0 and con_count ne 0 then AUR = con_sum / con_count; 
data m003_30d; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count AUR;
set m0022;by symbol; if last.symbol; con_sum =sum(of inst1-inst47); con_count = sumabs(of inst1-inst47); 
if con_sum ne 0 and con_count ne 0 then AUR = con_sum / con_count; run;

 *- 1~1461일 동안 M002, M0021 만들어서 매일매일의 추세 기록;
data _aur_30day; set _aur_30day m003_30d; 
data _aur_60day; set _aur_60day m003_60d;
run;

data _tm2p; set d002c; if name = "BGF리테일"; run;



/*date60 변수 ~ date1461 변수에 date 값 입력함 */
/*data _tmp; set d002c; if name = "삼성전자";*/
/* data _null_; set _tmp; suffix=put(_n_,5.); call symput(cats('date',suffix), date); run; */




 /********************************/
/*         정식 매크로 START        */
/********************************/

options nonotes;
%macro aaa();
/*date60 변수 ~ date1461 변수에 date 값 입력함 */
data _tmp; set d002c; if name = "삼성전자";
 data _null_; set _tmp; suffix=put(_n_,5.); call symput(cats('date',suffix), date); run; 

*결과 들어갈 data 초기화; data _aur_60day; set _null_; data _aur_30day; set _null_; run;

  /*매크로 시작 - nday = 60 to 1461*/
    %DO j= 60 %TO 1461;/*1461;*/
    %put &j %sysfunc(putn(&&date&j, yymmdd10.));

    data m0011; set d002c; if &j - 59 <= nday <= &j; 
    data m0012; set d002c; if &j - 29 <= nday <= &j; run;

    *최근 60일의 애널리스트 긍정/부정 추세;
    data m0021; do _n_ = 1 by 1 until(last.symbol);   update m0011(obs=0) m0011; by symbol;  end; 
    do _n_ = 1 to _n_; output ; end ; 
    *최근 30일의 애널리스트 긍정/부정 추세;
    data m0022; do _n_ = 1 by 1 until(last.symbol);   update m0012(obs=0) m0012; by symbol; end; 
    do _n_ = 1 to _n_; output ; end ; run;

    * 30,60일동안의 추세 1줄로 요약;
    data m003_60d; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count AUR;
    set m0021;by symbol; if last.symbol; con_sum =sum(of inst1-inst47); con_count = sumabs(of inst1-inst47); 
    if con_sum ne 0 and con_count ne 0 then AUR = con_sum / con_count; 
    data m003_30d; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count AUR;
    set m0022;by symbol; if last.symbol; con_sum =sum(of inst1-inst47); con_count = sumabs(of inst1-inst47); 
    if con_sum ne 0 and con_count ne 0 then AUR = con_sum / con_count; run;

     *- 1~1461일 동안 M002, M0021 만들어서 매일매일의 추세 기록;
    data _aur_30day; set _aur_30day m003_30d; 
    data _aur_60day; set _aur_60day m003_60d;
    run;
  %end;
  proc sort data=_aur_30day; by name date; proc sort data=_aur_60day; by name date; run;
%mend; %aaa();
options notes;

/*
data con.AUR_30day_rolling; set _aur_30day;
data con.AUR_60day_rolling; set _aur_60day; run;
*/

/* RESTART* - Analyst Update Ratio(AUR)가 1 또는 -1일 때 주가추이 관찰*/

data _aur_30day;  set con.AUR_30day_rolling;  
data _aur_60day;    set con.AUR_60day_rolling;   run;

*AUR 2 --> count 및 sum 개선;
data _aur_30day2; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count AUR; 
set _aur_30day;  AUR = con_sum / con_count; 
data _aur_60day2; retain symbol name no sec_code sec_name date nday adj_p con_sum con_count AUR; 
set _aur_60day; AUR = con_sum / con_count; run;

/*
data con.AUR_30day_rolling;  set _aur_30day2;  
data con.AUR_60day_rolling;   set _aur_60day2;    run;
*/


/******RESTART******/
/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";

*먼저 컨센서스가 5개 이상일 때 AUR이 1또는 -1인 경우 index를 수집한 후, 그 index +-90일 데이터 수집;

data e001; set con.AUR_30day_rolling; if (aur = 1 or aur = -1) and con_count > 4; run;
/*data e002; set con.AUR_60day_rolling; if ( (aur < 0 and lag(aur) > 0) or (aur > 0 and lag(aur) < 0)) and con_count > 4; run;*/
/*data e001_tmp; set _aur_60day2; if symbol = "A051900"; keep symbol name date adj_p aur; run;*/
/*data e001_tmp; set _aur_30day2; if symbol = "A051900"; keep symbol name date adj_p aur; run;*/

data e003; keep symbol name date adj_p aur; run;

data e010; set _aur_60day2; proc sort data=e010; by no date; run;

***추정치 5개 이상만 남김;

***BIG MOVER 데이터 만들기; 
data _big_mover1; set _op_diff2; array inst(47) inst1-inst47;
do i = 1 to 47;
  if -10 < inst[i] < 10 then inst[i] = .;
end; drop i; run;
data _big_mover2; retain no symbol name sec_code sec_name date nday adj_P big_sum big_count;
set _big_mover1; array inst(47) inst1-inst47;
do i = 1 to 47;
  if inst[i] ne . and -10 >= inst[i] then inst[i] = -1;
  if inst[i] ne . and 10 <= inst[i] then inst[i] = 1;
end; drop i; big_sum =sum(of inst1-inst47); big_count = sumabs(of inst1-inst47); run;

/* DATA con._big_mover; set _big_mover2; run;*/
****BIG MOVER와 AUR 데이터 합치기;
proc sort data=_big_mover1; by no date; run;
proc sort data=con.AUR_60day_rolling; by no date; proc sort data=_big_mover2; by no date;
data __aur60_bigmover; retain no symbol name sec_code sec_name date nday adj_P con_sum con_count big_sum big_count;
merge con.AUR_60day_rolling _big_mover2(drop=inst1-inst47); 
by no date; 
run;
/*
data con._aur60_big_merged; set __aur60_bigmover; run;
*/



/******RESTART** AUR(60일)과 BIG MOVER 합친 데이터 사용해서 포트폴리오 분석****/
/******** PART 0 PATH 설정 ******/
%let path=D:\(강동훈)\(0.WORK)\03.이진우 Analyst 보고서_백테스팅(Full Version);
libname CON "&path.";
*** 60일 AUR과 big mover 합친 데이터 추정치 5개 이상만 남김;
data e020; set con._aur60_big_merged; drop inst1-inst47; if con_count > 4;
run;

*LAG_AUR, FWD_AUR 값 추가;
*lag(AUR); DATA e021; set e020; by no date; lag_AUR=lag(AUR); if first.no then lag_AUR = .;
*바로 위 row가 직전일 아니면 lag_AUR missing; if nday - lag(nday) > 1 then lag_AUR = .;
proc sort data=e021 out=e022; by no descending date; 
data e022; set e022; by no descending date; fwd_AUR = lag(AUR); if first.no then fwd_AUR = .; 
*바로 위 row가 직전일 아니면 lag_AUR missing; if nday - lag(nday) > 1 then fwd_AUR = .;
proc sort data=e022; by no date; run;
data e023; retain no symbol name sec_code sec_name date adj_p con_sum con_count 
lag_AUR AUR big_sum big_count; drop fwd_aur;
set e022; run;


/*
data con._aurbig_mgd_lgfwd; set e023; run;
*/

*포트폴리오 짜보기;


data e023; set con._aurbig_mgd_lgfwd; run;

***THRESHOLD = +1/-1;
***THRESHOLD = +1/-1;
***THRESHOLD = +1/-1;
***THRESHOLD = +1/-1;

*매수신호 개선: lag(AUR) 값이 threshold * (-1) 보다 작고, AUR값이 threshold보다 커지면 매수신호;
data q0010_low; set e023; if lag_AUR ne . and AUR ne . and  lag_AUR le -1 and AUR > -1; signal = "B";
*매도신호 개선: lag(AUR) 값이 threshold보다 크고, AUR값이 threshold보다 작으면 매수신호;
data q0010_high; set e023; if lag_AUR ne . and AUR ne . and lag_AUR ge 1 and AUR < 1; signal = "S";
data q0010; set q0010_low q0010_high;  proc sort data=q0010; by no symbol name date; run;


***THRESHOLD = +1/-1 시그널 데이터셋(Sig_t1) 만들기; 
data sig_t1; retain no symbol name lag_AUR AUR big_sum con_count signal; set q0010; 
keep no symbol name date adj_p lag_AUR AUR big_sum con_count signal; rename adj_p = P; run;

*index 추가; proc sql; CREATE TABLE sig_t1_1 AS SELECT DISTINCT  s.*, i.k200 AS k200
FROM sig_t1 AS s, _k200_index AS i WHERE s.date eq i.date; quit; run;

*포트폴리오 수익률 계산: 신호 후 1W / 1M / 3M / 6M 보유;
*1w, 1m, 3m, 6m 이후 날짜를 변수로 생성 후 해당날짜의 K200 / 주식 수정종가 붙이기;

*add 1W date (date1w); data sig_t1_2; set sig_t1_1; date1w = intnx('week',date,1,'same'); 
format date1w yymmdd10.; run;
*1주일 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t1_3 
AS SELECT DISTINCT  s.*, p.adj_P as P_1w, i.k200 AS k200_1w
FROM  sig_t1_2 AS s, _k200_price as p, _k200_index AS i
WHERE s.date1w eq i.date and s.symbol eq p.symbol and s.date1w eq p.date; quit; run;

*add 1M date; data sig_t1_3; set sig_t1_3; date1m = intnx('month',date,1,'same'); 
format date1m yymmdd10.; run;
*1달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t1_4 
AS SELECT DISTINCT  s.*, p.adj_P as P_1m, i.k200 AS k200_1m
FROM  sig_t1_3 AS s, _k200_price as p, _k200_index AS i
WHERE s.date1m eq i.date and s.symbol eq p.symbol and s.date1m eq p.date; quit; run;


*add 3M date; data sig_t1_4; set sig_t1_4; date3m = intnx('month',date,3,'same'); 
format date3m yymmdd10.; run;
*3달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t1_5 
AS SELECT DISTINCT  s.*, p.adj_P as P_3m, i.k200 AS k200_3m
FROM  sig_t1_4 AS s, _k200_price as p, _k200_index AS i
WHERE s.date3m eq i.date and s.symbol eq p.symbol and s.date3m eq p.date; quit; run;

*add 6M date; data sig_t1_5; set sig_t1_5; date6m = intnx('month',date,6,'same'); 
format date6m yymmdd10.; run;
*6달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t1_6 
AS SELECT DISTINCT  s.*, p.adj_P as P_6m, i.k200 AS k200_6m
FROM  sig_t1_5 AS s, _k200_price as p, _k200_index AS i
WHERE s.date6m eq i.date and s.symbol eq p.symbol and s.date6m eq p.date; quit; run;

*날아간 데이터 합치기 (1개월 + 3개월 + 6개월 sig_t1_4 sig_t1_5 sig_t1_6);
proc sort data=sig_t1_4; by no date; proc sort data=sig_t1_5; by no date; proc sort data=sig_t1_6; by no date;
data sig_t1_7; merge sig_t1_4 sig_t1_5(keep= no date date3m p_3m k200_3m) sig_t1_6(keep= no date date6m p_6m k200_6m);
by no date; run;

*수익률 계산;
data sig_t1_8; retain no Symbol name lag_AUR AUR big_sum con_count signal DATE date1w date1m 
date3m date6m P P_1w P_1m P_3m P_6m k200 k200_1w k200_1m k200_3m k200_6m;
set sig_t1_7;

ret_mkt_1w = (k200_1w - k200) * 100 / k200;
ret_mkt_1m = (k200_1m - k200) * 100 / k200;
ret_mkt_3m = (k200_3m - k200) * 100 / k200;
ret_mkt_6m = (k200_6m - k200) * 100 / k200;

if signal = "B" then do;
  absret_1w = (P_1w - P) * 100 / P; 
  absret_1m = (P_1m - P) * 100 / P;
  absret_3m = (P_3m - P) * 100 / P;
  absret_6m = (P_6m - P) * 100 / P;
    
  relret_1w = absret_1w - ret_mkt_1w;
  relret_1m = absret_1m - ret_mkt_1m;
  relret_3m = absret_3m - ret_mkt_3m;
  relret_6m = absret_6m - ret_mkt_6m;
end;
else if signal = "S" then do;
  absret_1w = (P - P_1w) * 100 / P;
  absret_1m = (P - P_1m) * 100 / P;
  absret_3m = (P - P_3m) * 100 / P;
  absret_6m = (P - P_6m) * 100 / P;

  *SELL SIGNAL에서는 MARKET 대비 UNDER 난 수익률로 교체;
  relret_1w = absret_1w + ret_mkt_1w;
  relret_1m = absret_1m + ret_mkt_1m;
  relret_3m = absret_3m + ret_mkt_3m;
  relret_6m = absret_6m + ret_mkt_6m;
end;


run;



***THRESHOLD = +.9/-.9;
***THRESHOLD = +.9/-.9;
***THRESHOLD = +.9/-.9;
***THRESHOLD = +.9/-.9;
*매수신호 개선: lag(AUR) 값이 threshold * (-1) 보다 작고, AUR값이 threshold보다 커지면 매수신호;
data q0009_low; set e023; if lag_AUR ne . and AUR ne . and  lag_AUR le -0.9 and AUR > -0.9; signal = "B";
*매도신호 개선: lag(AUR) 값이 threshold보다 크고, AUR값이 threshold보다 작으면 매수신호;
data q0009_high; set e023; if lag_AUR ne . and AUR ne . and lag_AUR ge 0.9 and AUR < 0.9; signal = "S";
data q0009; set q0009_low q0009_high;  proc sort data=q0009; by no symbol name date; run;


***THRESHOLD = +.9/-.9 시그널 데이터셋(Sig_t09) 만들기; 
data sig_t09; retain no symbol name lag_AUR AUR big_sum con_count signal; set q0009; 
keep no symbol name date adj_p lag_AUR AUR big_sum con_count signal; rename adj_p = P; run;

*index 추가; proc sql; CREATE TABLE sig_t09_1 AS SELECT DISTINCT  s.*, i.k200 AS k200
FROM sig_t09 AS s, _k200_index AS i WHERE s.date eq i.date; quit; run;

*포트폴리오 수익률 계산: 신호 후 1W / 1M / 3M / 6M 보유;
*1w, 1m, 3m, 6m 이후 날짜를 변수로 생성 후 해당날짜의 K200 / 주식 수정종가 붙이기;

*add 1W date (date1w); data sig_t09_2; set sig_t09_1; date1w = intnx('week',date,1,'same'); 
format date1w yymmdd10.; run;
*1주일 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t09_3 
AS SELECT DISTINCT  s.*, p.adj_P as P_1w, i.k200 AS k200_1w
FROM  sig_t09_2 AS s, _k200_price as p, _k200_index AS i
WHERE s.date1w eq i.date and s.symbol eq p.symbol and s.date1w eq p.date; quit; run;

*add 1M date; data sig_t09_3; set sig_t09_3; date1m = intnx('month',date,1,'same'); 
format date1m yymmdd10.; run;
*1달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t09_4 
AS SELECT DISTINCT  s.*, p.adj_P as P_1m, i.k200 AS k200_1m
FROM  sig_t09_3 AS s, _k200_price as p, _k200_index AS i
WHERE s.date1m eq i.date and s.symbol eq p.symbol and s.date1m eq p.date; quit; run;


*add 3M date; data sig_t09_4; set sig_t09_4; date3m = intnx('month',date,3,'same'); 
format date3m yymmdd10.; run;
*3달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t09_5 
AS SELECT DISTINCT  s.*, p.adj_P as P_3m, i.k200 AS k200_3m
FROM  sig_t09_4 AS s, _k200_price as p, _k200_index AS i
WHERE s.date3m eq i.date and s.symbol eq p.symbol and s.date3m eq p.date; quit; run;

*add 6M date; data sig_t09_5; set sig_t09_5; date6m = intnx('month',date,6,'same'); 
format date6m yymmdd10.; run;
*6달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t09_6 
AS SELECT DISTINCT  s.*, p.adj_P as P_6m, i.k200 AS k200_6m
FROM  sig_t09_5 AS s, _k200_price as p, _k200_index AS i
WHERE s.date6m eq i.date and s.symbol eq p.symbol and s.date6m eq p.date; quit; run;

*날아간 데이터 합치기 (1개월 + 3개월 + 6개월 sig_t09_4 sig_t09_5 sig_t09_6);
proc sort data=sig_t09_4; by no date; proc sort data=sig_t09_5; by no date; proc sort data=sig_t09_6; by no date;
data sig_t09_7; merge sig_t09_4 sig_t09_5(keep= no date date3m p_3m k200_3m) sig_t09_6(keep= no date date6m p_6m k200_6m);
by no date; run;

*수익률 계산;
data sig_t09_8; retain no Symbol name lag_AUR AUR big_sum con_count signal DATE date1w date1m 
date3m date6m P P_1w P_1m P_3m P_6m k200 k200_1w k200_1m k200_3m k200_6m;
set sig_t09_7;
ret_mkt_1w = (k200_1w - k200) * 100 / k200; ret_mkt_1m = (k200_1m - k200) * 100 / k200;
ret_mkt_3m = (k200_3m - k200) * 100 / k200; ret_mkt_6m = (k200_6m - k200) * 100 / k200;
if signal = "B" then do; absret_1w = (P_1w - P) * 100 / P;  
absret_1m = (P_1m - P) * 100 / P; absret_3m = (P_3m - P) * 100 / P; absret_6m = (P_6m - P) * 100 / P; end;
else if signal = "S" then do; absret_1w = (P - P_1w) * 100 / P;
absret_1m = (P - P_1m) * 100 / P; absret_3m = (P - P_3m) * 100 / P; absret_6m = (P - P_6m) * 100 / P;end;
relret_1w = absret_1w - ret_mkt_1w; relret_1m = absret_1m - ret_mkt_1m; relret_3m = absret_3m - ret_mkt_3m;
relret_6m = absret_6m - ret_mkt_6m; run;





***THRESHOLD = +.8/-.8;
***THRESHOLD = +.8/-.8;
***THRESHOLD = +.8/-.8;
***THRESHOLD = +.8/-.8;

*매수신호 개선: lag(AUR) 값이 threshold * (-1) 보다 작고, AUR값이 threshold보다 커지면 매수신호;
data q0008_low; set e023; if lag_AUR ne . and AUR ne . and  lag_AUR le -0.8 and AUR > -0.8; signal = "B";
*매도신호 개선: lag(AUR) 값이 threshold보다 크고, AUR값이 threshold보다 작으면 매수신호;
data q0008_high; set e023; if lag_AUR ne . and AUR ne . and lag_AUR ge 0.8 and AUR < 0.8; signal = "S";
data q0008; set q0008_low q0008_high;  proc sort data=q0008; by no symbol name date; run;


***THRESHOLD = +.9/-.9 시그널 데이터셋(Sig_t08) 만들기; 
data sig_t08; retain no symbol name lag_AUR AUR big_sum con_count signal; set q0008; 
keep no symbol name date adj_p lag_AUR AUR big_sum con_count signal; rename adj_p = P; run;

*index 추가; proc sql; CREATE TABLE sig_t08_1 AS SELECT DISTINCT  s.*, i.k200 AS k200
FROM sig_t08 AS s, _k200_index AS i WHERE s.date eq i.date; quit; run;

*포트폴리오 수익률 계산: 신호 후 1W / 1M / 3M / 6M 보유;
*1w, 1m, 3m, 6m 이후 날짜를 변수로 생성 후 해당날짜의 K200 / 주식 수정종가 붙이기;

*add 1W date (date1w); data sig_t08_2; set sig_t08_1; date1w = intnx('week',date,1,'same'); 
format date1w yymmdd10.; run;
*1주일 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t08_3 
AS SELECT DISTINCT  s.*, p.adj_P as P_1w, i.k200 AS k200_1w
FROM  sig_t08_2 AS s, _k200_price as p, _k200_index AS i
WHERE s.date1w eq i.date and s.symbol eq p.symbol and s.date1w eq p.date; quit; run;

*add 1M date; data sig_t08_3; set sig_t08_3; date1m = intnx('month',date,1,'same'); 
format date1m yymmdd10.; run;
*1달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t08_4 
AS SELECT DISTINCT  s.*, p.adj_P as P_1m, i.k200 AS k200_1m
FROM  sig_t08_3 AS s, _k200_price as p, _k200_index AS i
WHERE s.date1m eq i.date and s.symbol eq p.symbol and s.date1m eq p.date; quit; run;


*add 3M date; data sig_t08_4; set sig_t08_4; date3m = intnx('month',date,3,'same'); 
format date3m yymmdd10.; run;
*3달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t08_5 
AS SELECT DISTINCT  s.*, p.adj_P as P_3m, i.k200 AS k200_3m
FROM  sig_t08_4 AS s, _k200_price as p, _k200_index AS i
WHERE s.date3m eq i.date and s.symbol eq p.symbol and s.date3m eq p.date; quit; run;

*add 6M date; data sig_t08_5; set sig_t08_5; date6m = intnx('month',date,6,'same'); 
format date6m yymmdd10.; run;
*6달 후 K200 / 종가 matching; proc sql; CREATE TABLE sig_t08_6 
AS SELECT DISTINCT  s.*, p.adj_P as P_6m, i.k200 AS k200_6m
FROM  sig_t08_5 AS s, _k200_price as p, _k200_index AS i
WHERE s.date6m eq i.date and s.symbol eq p.symbol and s.date6m eq p.date; quit; run;

*날아간 데이터 합치기 (1개월 + 3개월 + 6개월 sig_t08_4 sig_t08_5 sig_t08_6);
proc sort data=sig_t08_4; by no date; proc sort data=sig_t08_5; by no date; proc sort data=sig_t08_6; by no date;
data sig_t08_7; merge sig_t08_4 sig_t08_5(keep= no date date3m p_3m k200_3m) sig_t08_6(keep= no date date6m p_6m k200_6m);
by no date; run;

*수익률 계산;
data sig_t08_8; retain no Symbol name lag_AUR AUR big_sum con_count signal DATE date1w date1m 
date3m date6m P P_1w P_1m P_3m P_6m k200 k200_1w k200_1m k200_3m k200_6m;
set sig_t08_7;  ret_mkt_1w = (k200_1w - k200) * 100 / k200; ret_mkt_1m = (k200_1m - k200) * 100 / k200;
ret_mkt_3m = (k200_3m - k200) * 100 / k200; ret_mkt_6m = (k200_6m - k200) * 100 / k200;
if signal = "B" then do;  absret_1w = (P_1w - P) * 100 / P; absret_1m = (P_1m - P) * 100 / P;
absret_3m = (P_3m - P) * 100 / P; absret_6m = (P_6m - P) * 100 / P;end;
else if signal = "S" then do; absret_1w = (P - P_1w) * 100 / P; absret_1m = (P - P_1m) * 100 / P;
absret_3m = (P - P_3m) * 100 / P; absret_6m = (P - P_6m) * 100 / P;end;
relret_1w = absret_1w - ret_mkt_1w; relret_1m = absret_1m - ret_mkt_1m;
relret_3m = absret_3m - ret_mkt_3m; relret_6m = absret_6m - ret_mkt_6m; run;




*rolling regression 매크로 없이 하기;
%let roll_step = 5;

data e023_roll;
   set e023(keep=symbol name date);
   by symbol; 
   retain firstdate;
   if first.symbol then firstdate=date;
   if last.symbol then do;
       lastdate=date;
 output;
   end;
   format firstdate yymmdd10. lastdate yymmdd10.;
run;

data _roll_dates(rename=(date=roll_date));    set e023_roll;    
date=intnx('day', firstdate, &roll_step);
do while(date<=lastdate);   output;   date=intnx('day', date, 1);   end;run;
*For each roll_date, I then get the list of the 24 dates from which that roll_date will use data.;
data _roll_dates;
   set _roll_dates;   date=roll_date - 1;   i=1;
   do while(i <= &roll_step);  output;
      date=intnx('day', date, -1); i=i+1; end;
format date yymmdd10.; drop firstdate lastdate; run;
proc datasets library=work nolist;  modify _roll_dates;  attrib _all_ label=''; quit;
  


*row 수 observation 수 sympux로 변수에 넣기(A05데이터셋, nrows변수);
data _NULL_;
  if 0 then set A05 nobs=n;
  call symputx('nobs',n);
  stop;
run;
%put nobs=&nobs;


 %put &ticker1;
 %put %sysfunc(putn(&start_date1,yymmdd10.));
 %put %sysfunc(putn(&end_date1,yymmdd10.));





*symput 사용해서 _cb_event 데이터셋의 값들을 변수입력;

/*ticker1 변수 ~ ticker390 변수에 under_eq_ticker 값 입력함 */
 data _null_; set a005_issuers; suffix=put(_n_,5.); call symput(cats('issuer',suffix), report_issuer); run; 

/*name1 변수 ~ name390 변수에 under_equity 값 입력함 */
 data _null_; set a005_issuers; suffix=put(_n_,5.); call symput(cats('name',suffix), under_equity); run; 

/*start_date1 변수 ~ start_date390 변수에 Adate_90wd_prev 값 입력함 */
 data _null_; set _cb_event; suffix=put(_n_,5.); call symput(cats('start_date',suffix), Adate_90wd_prev); run; 

/*end_date1 변수 ~ end_date390 변수에 Adate_90wd_prev 값 입력함 */
 data _null_; set _cb_event; suffix=put(_n_,5.); call symput(cats('end_date',suffix), Adate_90wd_next); run; 

 %put &ticker1 &name1 %sysfunc(putn(&start_date1,yymmdd10.)) 
%sysfunc(putn(&end_date1,yymmdd10.));


/**********************************************************/
/*  (정식 매크로 START) 각 event 별로 event 전후 90일 데이터 뽑기 */
/**********************************************************/
options nonotes;
%macro aaa();
*결과 들어갈 data 초기화; data _cb_event_181wd;  set _null_;  run;
    %DO j= 1 %TO 390;
  
  %put &&ticker&j &&name&j %sysfunc(putn(&&start_date&j,yymmdd10.)) 
  %sysfunc(putn(&&end_date&j,yymmdd10.));

         *A112데이터(CB발행데이터) 에서 CB_EVENT_ID=#n인 데이터만 불러와 M001에 저장;
        data M001; set _cb_event; if CB_EVENT_ID=&j; run;

         *A201(데이터가이드 데이터) 에서 ticker#n이고 date가 start_date#n과 end_date#n 사이인 데이터 불러와 m002에 저장;
         data M002;set A301; 
            if symbol = "&&ticker&j" and date ge &&start_date&j and date le &&end_date&j;
        CB_EVENT_ID=&j; run;

        *date를 event_date으로 이름 바꾸고 event_date_idx변수에 -90~+90까지의 인덱스 부여;
        data M002_d; set M002; rename date=event_date;
        event_date_idx=_n_; event_date_idx = event_date_idx - 91; run;
        data M002_d2; retain symbol name event_date event_date_idx; set M002_d; run;

        *M001을 M002데이터와 합침;
        data M003; merge M001 M002_d2; by CB_EVENT_ID; run;

    data _cb_event_181wd;  set _cb_event_181wd M003;  run;
    %END;
%mend;
%aaa();
options notes;


/*
data cb.cb_event_181wd; set _cb_event_181wd; run;
*/

/******RESTART******/
resetline; 
ods html close; 
ods graphics off; 
ods listing;
/******** PART 0 PATH 설정 ******/
*%let path=C:\Dropbox\(1.박사논문)\(조병우선배님)\SAS데이터분석\2016-08-07_Dataguide 데이터 불러오기;
%let path=C:\Users\KDH-MAC\Dropbox\(1.박사논문)\(조병우선배님)\SAS데이터분석\2016-08-07_Dataguide 데이터 불러오기;
libname CB "&path.";

data _cb_event_181wd; set cb.cb_event_181wd; run;

/* Announcement Date 1일 전 데이터*/
data b001; set _cb_event_181wd; if event_date_idx = -1; run;
/*
DATA cb.cb_event_1wd_prev; set b001; run;
*/

/* Table 1 분석 
retain CB_EVENT_ID issuer issuer_en issue_date under_equity under_eq_ticker exch exch_code;*/

data b002; set b001; 
*거래소 코드(KOSPI=1, KOSDAQ=0)부여;
IF exch="KSE" then kse = 1; ELSE kse=0;
IF exch="KOSDAQ" THEN kosdaq=1; else kosdaq=0;
*시가총액 백만원 단위로; market_cap = market_cap / 1000000;
*거래대금 백만원 단위로; vol_won = vol_won / 1000000;
*부채비율 자본잠식 제외; if debt_ratio = 999999 then debt_ratio = .;
*issuanece/market_cap 변수 추가; issue_mktcap_ratio = issuance_krw_mm / market_cap * 100;
*short_interest 결측치일 경우 0 추가; if short_interest = . then short_interest = 0;
*short_interest/shares outstanding ratio 추가; siso_ratio = short_interest / common_shares_outstanding * 100;
run;
proc means data=b002 noprint; 
var market_cap kse kosdaq Debt_CommEq_ratio vol_won beta issuance_krw_mm
issue_mktcap_ratio short_interest siso_ratio cr_moody_num cr_s_p_num cr_fitch_num
cr_kis_num cr_nice_num cr_kr_num; 
output out=b002_means; output out=b002_median median=;run;
data b002_means2;
   set b002_means b002_median(in=in2);
   by _type_;
   if in2 then _STAT_ = 'MEDIAN';
   run;


*30거래일 전 대비 대차잔고 거래증감 변수 만들기;
DATA b010; set _cb_event_181wd; if event_date_idx = 0;
data b011; set _cb_event_181wd; if event_date_idx = 0;  
  keep cb_event_id -- exch  short_interest; rename short_interest = si_0wd; 
DATA b012; set _cb_event_181wd; if event_date_idx = -30; 
  keep short_interest; rename short_interest = si_m_30wd;
data b013; merge b010 b011 b012; delta_si = (si_m_30wd - si_0wd) / si_m_30wd;

*4개 포트폴리오로 분류(결측값 88개); /*proc freq data=b013; tables delta_si; run;*/
*PROC RANK 사용;
proc rank data=b013 out=b014 groups = 4 ;  * 4 portfolios ;  var delta_si;  ranks delta_si_rank;
run;




































/* 매크로 예제 부분 시작*/

 *A112데이터(CB발행데이터) 에서 CB_EVENT_ID=#n인 데이터만 불러와 M001에 저장;
data M001; set _cb_event; if CB_EVENT_ID=1; run;

 *A201(데이터가이드 데이터) 에서 ticker#n이고 date가 start_date#n과 end_date#n 사이인 데이터 불러와 m002에 저장;
 data M002;set A301; if symbol = "&ticker1" and date ge &start_date1 and date le &end_date1;
CB_EVENT_ID=1; run;

*date를 event_date으로 이름 바꾸고 event_date_idx변수에 -90~+90까지의 인덱스 부여;
data M002_d; set M002; rename date=event_date;
event_date_idx=_n_; event_date_idx = event_date_idx - 91; run;
data M002_d2; retain symbol name event_date event_date_idx; set M002_d; run;

*M001을 M002데이터와 합침;
data M003; merge M001 M002_d2; by CB_EVENT_ID; run;

/* 매크로 예제 부분 끝*/



data a005; set con.a005; proc freq data=a005 noprint; table report_issuer / out=a005_issuers; 
data a005_issuers; retain n; set a005_issuers; n=_n_;
/*issuer1 ~ issuer50 변수에 증권사명 입력*/
data _null_; set a005_issuers; suffix=put(_n_,5.); call symput(cats('issuer',suffix), report_issuer); run; 
%put &issuer1 &issuer2 &issuer3 &issuer4 &issuer5 &issuer6 &issuer7 &issuer8 &issuer9 &issuer10;
run;




data _sector;
  infile "&path.\k200업종.csv" dsd lrecl=99999 firstobs=10;
  informat symbol $7. name $30. sec_code $7. sec_name $30.;
  input symbol name sec_code sec_name;
run;







%put &i of &nobs : %sysfunc(trim(&name1)) &gyulsan1 &issuer1 &analyst1 %sysfunc(putn(&nowday1,yymmdd10.)); 
