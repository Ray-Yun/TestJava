package controller;
public class FTP_fixed_code {

	import java.io.File;
	import java.io.IOException;
	import java.nio.file.Files;
	import java.nio.file.Path;
	import java.nio.file.Paths;
	import java.time.LocalDate;
	import java.time.format.DateTimeFormatter;
	import java.util.ArrayList;
	import java.util.Comparator;
	import java.util.Date;
	import java.util.HashMap;
	import java.util.List;
	import java.util.Map;
	import java.util.Optional;
	import java.util.concurrent.ConcurrentHashMap;
	import java.util.concurrent.ConcurrentMap;
	import java.util.stream.Collectors;

	import org.quartz.DisallowConcurrentExecution;
	import org.quartz.Job;
	import org.quartz.JobDataMap;
	import org.quartz.JobDetail;
	import org.quartz.JobExecutionContext;
	import org.quartz.JobExecutionException;
	import org.quartz.JobKey;
	import org.quartz.PersistJobDataAfterExecution;
	import org.quartz.Scheduler;

	import com.ofss.fcc.EODFileEJB;
	import com.ofss.fcc.EODFileEJBLocal;
	import com.ofss.fcc.common.FCApplicationGlobals;
	import com.ofss.fcc.common.FCServiceLocator;
	import com.ofss.fcc.exception.FCSchedulerException;
	import com.ofss.fcc.logger.ApplicationLog;
	import com.ofss.fcc.utility.FCUtility;

	@PersistJobDataAfterExecution
	@DisallowConcurrentExecution
	public class FTPSchedulerQrtz implements Job {
		private static String logFileName = "";
		private static boolean logReq = false;
		
		private static String userId = "Scheduler_FTP";
		public String userId2 = "Scheduler_FTP2";
		private String userId3 = "Scheduler_FTP3";
		public static String userId5 = "Scheduler_FTP5";
		private static String userId4 = "Scheduler_FTP4";
		

		
		private String jobName = "";
		private final static Integer DEFAULT_NO_OF_THREADS = Integer.valueOf(1);

		/**
		 * write log
		 * @param String
		 * 
		 */
		public void dbg(String msg) {
			//ApplicationLog.getInstance().writeLog(getUserId(), "FTPSchedulerQrtz." + msg);
			ApplicationLog aaa = ApplicationLog.getInstance();	
			if(aaa!=null) {
			String userId = this.getUserId();
			aaa.writeLog(userId ,"FTPSchedulerQrtz." + msg);
			}
		}

		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException {
			ConcurrentMap<String, Exception> exceptions = new ConcurrentHashMap<>();
			try {
				this.dbg("aa");
//				final String jobDetailName = context.getJobDetail().getKey().getName();
				
				JobDetail jobDetail = context.getJobDetail();
				JobKey jobkey = jobDetail.getKey();
				final String jobDetailName = jobkey.getName();
				

				jobName = jobDetailName.substring(jobDetailName.indexOf(".") + 1, jobDetailName.length());
				setUserId("Scheduler_" + jobName);
				logFileName = jobName;
				//JobDataMap data = context.getJobDetail().getJobDataMap();
				jobDetail=context.getJobDetail();
				JobDataMap data=jobDetail.getJobDataMap();
				
				
				data.put("ErrorOccured", "no");
				String jndiName = data.getString("dsn");
				HashMap params = (HashMap) data.get("params");
				Scheduler scheduler = context.getScheduler();
		//		final boolean logReq= Optional.ofNullable(data.getString("LoggingReqd")).filter("Y"::equalsIgnoreCase)
		//			.isPresent();
				String st=data.getString("LoggingReqd");
				Optional<String> op = Optional.ofNullable(st);
				Optional<String> op2 = op.filter("Y"::equalsIgnoreCase) ;
				Optional<String> op3 = op2.filter( f -> f.equalsIgnoreCase("Y"));
				final boolean logReq=  op3.isPresent();
			//	ApplicationLog.getInstance().setDebug(getUserId(), logReq);
				ApplicationLog log= ApplicationLog.getInstance();
				String ee =this.getUserId();
				log.setDebug(ee, logReq);
				
				
				dbg("execute-->**********Starts_20201008*********");
				dbg("execute-->jobName name: " + jobName);
				dbg("execute-->logReq: " + logReq);
				dbg("execute-->logFileName: " + logFileName);
				dbg("execute-->jndiName: " + jndiName);

		//		final String tempPath = ((String) Optional.ofNullable(params.get("TEMP_PATH"))
		//				.orElse("/weblogic/FLEXCUBE_ODS/"));
				String tp =(String)params.get("TEMP_PATH");
				Optional<String> tp2= Optional.ofNullable(tp);
				String tp3= tp2.orElse("/weblogic/FLEXCUBE_ODS/");
				final String tempPath=(String)tp3;
				// Type mismatch: cannot convert from Optional<String> to String?!
			
				
				File tempDir = new File(tempPath);
				if (!tempDir.exists()) {
					tempDir.mkdirs();
				}
				params.put("TEMP_PATH", tempPath);
				dbg("execute-->tempPath: " + tempPath);
				if ("LOCAL".equalsIgnoreCase(FCApplicationGlobals.getSchedLookupType())) {
					//EODFileEJBLocal eodFileEjbLocal = (EODFileEJBLocal) FCServiceLocator.getInstance()
				//			.getEJBInstance("FCUBS_EODFILEEJB_LOCAL");
					FCServiceLocator ff= FCServiceLocator.getInstance();
					EODFileEJBLocal eodFileEjbLocal=(EODFileEJBLocal)ff.getEJBInstance("FCUBS_EODFILEEJB_LOCAL");
					
					dbg("execute-->Calling the getEodMaster");
					List<Map<String, Object>> masters = eodFileEjbLocal.getEodMaster(jndiName, jobName, logReq);
					dbg(String.format("execute-->eodMaster.size()=%d", masters.size()));
					if (masters.size() > 0) {
						ConcurrentMap<LocalDate, List<Map<String, Object>>> gorupMap = masters.parallelStream().collect(
								Collectors.groupingByConcurrent(master -> (LocalDate) master.get("RUN_WORK_DATE")));
						gorupMap.entrySet().parallelStream().forEach(dateGroup -> {
							LocalDate workDate = dateGroup.getKey();
							
							final String dumpPath = tempPath.concat(File.separator)
									.concat(workDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
							
							File dumpDir = new File(dumpPath);
							if (!dumpDir.exists()) {
								dumpDir.mkdirs();
							}
							dateGroup.getValue().parallelStream() //
									.forEach(master -> {
										List<Map<String,Object>> details = new ArrayList<>();
										HashMap detailParams = new HashMap();
					
										detailParams.put("JOB_NO", master.get("JOB_NO"));
										detailParams.put("RUN_TABLE", master.get("RUN_TABLE"));
										try {
											details = eodFileEjbLocal.getEodDetail(jndiName, jobName, detailParams, logReq);
											
											dbg(String.format("execute-->eodDetails.size()=%d", masters.size()));
											
										} catch (Exception e) {
											exceptions.put("Exception", e);
										}

										ConcurrentMap<String, List<Map<String, Object>>> map = details.parallelStream()
												.collect(Collectors.groupingByConcurrent(
														detail -> (String) detail.get("RUN_FILE_NAME")));
										map.entrySet().parallelStream().forEach(entry -> {
											final String outputFileName = String
													.format(dumpPath.concat(File.separator).concat("%s"), entry.getKey());
											Path output = Paths.get(outputFileName);
											final String content = entry.getValue().stream()
													.sorted(Comparator.comparingInt(detail -> (int) detail.get("LINE_NUM")))
													.map(detail -> (String) detail.get("ALLDATA"))
													.collect(Collectors.joining("\n"));
											
											try {
												Files.write(output, content.getBytes());
											} catch (IOException e) {
												ApplicationLog.getInstance().writeStack(getUserId(), e);
												dbg("write file to " + output + " failed.");
											}
										});

										/* Updated status flag. */
										Optional.of(master).filter(m -> "N".equals(m.get("PRODUCTION_FILE")))
												.ifPresent(m -> m.put("PRODUCTION_FILE", "Y"));
										
										
										Optional.of(master).filter(m -> "Y".equals(m.get("RE_PRODUCTION")))
												.ifPresent(m -> m.put("RE_PRODUCTION", "N"));
										try {
											int result = eodFileEjbLocal.updateEodMaster(jndiName, master, logReq);
											if (result != 0) {
												dbg(String.format("execute-->master data changed."));
											}
										} catch (Exception e) {
											exceptions.put("Exception", e);
										}
									});
						});

						if (!exceptions.isEmpty()) {
							throw (Exception) exceptions.entrySet().stream().findAny().get();
						}
					}
				} else {
					dbg("execute-->EJB Remote Reference ");
					EODFileEJB eodFileEjb = (EODFileEJB) FCServiceLocator.getInstance()
							.getEJBRemoteInstance("com.ofss.fcc.EODFileEJB");
					dbg("execute-->Calling the getEodMaster");

					List<Map<String, Object>> masters = eodFileEjb.getEodMaster(jndiName, jobName, logReq);

					dbg(String.format("execute-->eodFileEjb.size()=%d", masters.size()));
					if (masters.size() > 0) {
						ConcurrentMap<LocalDate, List<Map<String, Object>>> gorupMap = masters.parallelStream().collect(
								Collectors.groupingByConcurrent(master -> (LocalDate) master.get("RUN_WORK_DATE")));
						gorupMap.entrySet().parallelStream().forEach(dateGroup -> {
							
							LocalDate workDate = dateGroup.getKey();
							final String dumpPath = tempPath.concat(File.separator)
									.concat(workDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
							File dumpDir = new File(dumpPath);
							if (!dumpDir.exists()) {
								dumpDir.mkdirs();
							}
							dateGroup.getValue().parallelStream() //
									.forEach(master -> {
										List<Map<String, Object>> details = new ArrayList<>();
										HashMap detailParams = new HashMap();
										detailParams.put("JOB_NO", master.get("JOB_NO"));
										detailParams.put("RUN_TABLE", master.get("RUN_TABLE"));
										try {
											details = eodFileEjb.getEodDetail(jndiName, jobName, detailParams, logReq);
											dbg(String.format("execute-->eodDetails.size()=%d", masters.size()));
										} catch (Exception e) {
											exceptions.put("Exception", e);
										}

										ConcurrentMap<String, List<Map<String, Object>>> map = details.parallelStream()
												.collect(Collectors.groupingByConcurrent(
														detail -> (String) detail.get("RUN_FILE_NAME")));
										map.entrySet().parallelStream().forEach(entry -> {
											final String outputFileName = String
													.format(dumpPath.concat(File.separator).concat("%s"), entry.getKey());
											Path output = Paths.get(outputFileName);
											final String content = entry.getValue().stream()
													.sorted(Comparator.comparingInt(detail -> (int) detail.get("LINE_NUM")))
													.map(detail -> (String) detail.get("ALLDATA"))
													.collect(Collectors.joining("\n"));
											try {
												Files.write(output, content.getBytes());
											} catch (IOException e) {
												ApplicationLog.getInstance().writeStack(getUserId(), e);
												dbg("write file to " + output + " failed.");
											}
										});

										/* Updated status flag. */
										Optional.of(master).filter(m -> "N".equals(m.get("PRODUCTION_FILE")))
												.ifPresent(m -> m.put("PRODUCTION_FILE", "Y"));
										Optional.of(master).filter(m -> "Y".equals(m.get("RE_PRODUCTION")))
												.ifPresent(m -> m.put("RE_PRODUCTION", "N"));
										try {
											int result = eodFileEjb.updateEodMaster(jndiName, master, logReq);
											if (result != 0) {
												dbg(String.format("execute-->master data changed."));
											}
										} catch (Exception e) {
											exceptions.put("Exception", e);
										}
									});
						});

						if (!exceptions.isEmpty()) {
							throw (Exception) exceptions.entrySet().stream().findAny().get();
						}

					}
				}
				dbg("Calling FCUploadFileHandler ");
				FCUploadFileHandler handler = new FCUploadFileHandler(jobName, logReq, getUserId());
				handler.processMessage(params);
				dbg("scheduler name: " + scheduler.getSchedulerName());

				data.put("offset", "1");
				data.put("LogInfo", "current time of firing is:" + new Date(System.currentTimeMillis()));
				dbg("execute-->Job " + jobName + " exected successfully");
			} catch (

			FCSchedulerException e) {
				String errorMsg = e.getErrMsg();
				ApplicationLog.getInstance().setDebug("SchedulerLog", true);
				ApplicationLog.getInstance().writeLog("SchedulerLog",
						"Error Occured while processing " + jobName + " Job; Error code:" + e.getErrCode()
								+ " Error Message:" + errorMsg + ".Please refer " + getUserId() + ".log for error>>>>>");
				ApplicationLog.getInstance().writeLog("SchedulerLog",
						"JOB STATUS ::: Error Occured while processing " + jobName + " job.");
				ApplicationLog.getInstance().writeLog("SchedulerLog", "JOB ERROR  ::: " + e.getErrCode() + "~" + errorMsg
						+ ".Refer " + getUserId() + ".log for more details.");
			} catch (Exception e) {
				ApplicationLog.getInstance().writeStack(getUserId(), e);
				String errorMsg = FCUtility.getErrorDesc("SC-00003");
				ApplicationLog.getInstance().setDebug("SchedulerLog", true);
				ApplicationLog.getInstance().writeLog("SchedulerLog",
						"JOB STATUS ::: Error Occured while processing " + jobName + " job.");
				ApplicationLog.getInstance().writeLog("SchedulerLog", "JOB ERROR  ::: SC-00003~" + errorMsg);
				ApplicationLog.getInstance().writeLog("SchedulerLog",
						"JOB STATE ::: Job Paused. Refer " + getUserId() + ".log for more details.");
				ApplicationLog.getInstance().writeLog("SchedulerLog",
						"JOB ACTION :::: Resolve the issue and Resume the JOB>>>>>");
				JobDataMap data = context.getJobDetail().getJobDataMap();
				data.put("ErrorOccured", "yes");
				data.put("errorInfo", errorMsg);
			} finally {
				dbg("**********End*********");
				ApplicationLog.getInstance().flush("SchedulerLog");
				ApplicationLog.getInstance().flush(getUserId());
			}
		}


		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}
		public String getUserId3() {
			return userId3;
		}
		public static String getUserId4() {
			// TODO Auto-generated method stub
			return userId4;
		}
	}
}
