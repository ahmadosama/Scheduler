using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo.Agent;
using Microsoft.SqlServer.Management;
using Microsoft.SqlServer.Server;
using System.Security.Cryptography;
using Crypto;
namespace Scheduler
{
    public class SQLAgentJob
    {
       private string JobServer="",userid ="",password="",authtype="";
       public Server SourceServer { get; set; }
       private Guid _SQLJobId;
       private Job _sqljob;
       public Guid SQLJobID { get { return this._SQLJobId; } }
     
       
       
        public SQLAgentJob(string _jobserver,string _userid,string _password,string _authtype)
        {
            JobServer=_jobserver;
            userid=_userid;
            password=_password;
            authtype=_authtype;
            SourceServer=fn_DBConnect();
        }

        /// <summary>
        /// returns connection to the server
        /// </summary>
        /// <returns></returns>
         private Server fn_DBConnect()
         {
             Server srv;
             ServerConnection sc = new ServerConnection() ;
             
             if(!string.IsNullOrEmpty(JobServer))
             {
                   sc= new ServerConnection(JobServer);
             }

             if(authtype=="Windows")
             {
                 sc.LoginSecure=true;
                 srv = new Server(sc);
             }else
             {
                 sc.LoginSecure=false;
                 sc.Login = userid;
                 sc.Password=Crypto.Crypto.Decrypt(password,true);
                 
                 sc.DatabaseName="master";
                 srv = new Server(sc);
             }
             return srv;
         }


         public void SQLJob(string jobname)
         {

             
             _sqljob = new Job(SourceServer.JobServer, jobname);

             _sqljob.Create();
             _sqljob.ApplyToTargetServer("(local)");
             _SQLJobId = _sqljob.JobID;
         }

        public void SQLJobStep(string command,string sqljobstepname="")
        {
             
             JobStep _sqljobstep = new JobStep(_sqljob, sqljobstepname);
             _sqljobstep.Command = command;

             _sqljobstep.OnSuccessAction = StepCompletionAction.QuitWithSuccess;

             _sqljobstep.OnFailAction = StepCompletionAction.QuitWithFailure;

            _sqljobstep.SubSystem = AgentSubSystem.CmdExec;
            
            _sqljobstep.Create();
           
        }

        public void SQLJobSchedule(string jobschedulename, string frequency, string frequencysubdaytype,
            int frequencysubdayinterval, DateTime startdate, DateTime? enddate, TimeSpan starttime, TimeSpan? endtime,int frequencyinterval,int frequencyrecurrinterval,string daysofweeks)
        {
            try
            {
               
                JobSchedule _sqljobschedule = new JobSchedule(_sqljob, jobschedulename);
                int _weeklyfrequencyinterval=0;
                
                switch (frequency)
                {
                    case "Daily": _sqljobschedule.FrequencyTypes = FrequencyTypes.Daily;
                        _sqljobschedule.FrequencyInterval = frequencyinterval;
                            ;
                        break;
                    case "OneTime": _sqljobschedule.FrequencyTypes = FrequencyTypes.OneTime;
                        break;
                    case "Monthly": _sqljobschedule.FrequencyTypes = FrequencyTypes.Monthly;
                        break;
                    case "Weekly": _sqljobschedule.FrequencyTypes = FrequencyTypes.Weekly;
                        if(string.IsNullOrEmpty(daysofweeks))
                        {
                            throw new ApplicationException("Days of week must be specified for Weekly frequency");
                            
                        }
                        string[] _daysofweeks = daysofweeks.Split(',');
                
                            foreach(string _day in _daysofweeks)
                                {
                                    _weeklyfrequencyinterval = _weeklyfrequencyinterval + fn_mapdaytofrequencyinterval(_day);
                                }
                            _sqljobschedule.FrequencyInterval = _weeklyfrequencyinterval;
                        break;
                    default: throw new ApplicationException("frequency can only be Onetime/Daily/Weekly.");
                        
                }
                //Set properties to define the schedule frequency, and duration. 
                switch (frequencysubdaytype)
                {
                    case "Hour": _sqljobschedule.FrequencySubDayTypes = FrequencySubDayTypes.Hour;
                        break;
                    case "Minute": _sqljobschedule.FrequencySubDayTypes = FrequencySubDayTypes.Minute;
                        break;
                    case "Seconds": _sqljobschedule.FrequencySubDayTypes = FrequencySubDayTypes.Second;
                        break;
                    case "Once": _sqljobschedule.FrequencySubDayTypes = FrequencySubDayTypes.Once;
                        break;
                    default: _sqljobschedule.FrequencySubDayTypes = FrequencySubDayTypes.Once;
                        break;
                }

                
                _sqljobschedule.FrequencyRecurrenceFactor = frequencyrecurrinterval;
                _sqljobschedule.FrequencySubDayInterval = frequencysubdayinterval;
                
                _sqljobschedule.ActiveStartTimeOfDay = starttime;
                _sqljobschedule.ActiveStartDate = startdate;
                if (enddate != null)
                {
                    _sqljobschedule.ActiveEndDate = enddate ?? DateTime.MaxValue;
                }
                if (endtime != null)
                {
                    _sqljobschedule.ActiveEndTimeOfDay = endtime ?? TimeSpan.MaxValue;
                }
                _sqljobschedule.Create();

            }catch(Exception ex)
            {
                throw ex;
            }

        }
        
        private int fn_mapdaytofrequencyinterval(string day)
        {
            int _frequencyinterval=0;
            switch (day)
            {
                case "Monday": _frequencyinterval = 2;
                    break;
                case "Tuesday": _frequencyinterval = 4;
                    break;
                case "Wednesday": _frequencyinterval = 8;
                    break;
                case "Thursday": _frequencyinterval = 16;
                    break;
                case "Friday": _frequencyinterval = 32;
                    break;
                case "Saturday": _frequencyinterval = 64;
                    break;
                case "Sunday": _frequencyinterval = 1;
                    break;
                default:
                    break;
            }
            return _frequencyinterval;
        }
    }
}
