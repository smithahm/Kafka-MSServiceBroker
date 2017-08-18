
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Threading;
using System.Data;

namespace ServiceBrokerToKafka
{
  internal class ServiceBrokerUtils
  {
    public static byte[]  GetMessage(string queueName, SqlConnection con, TimeSpan timeout)
    {
      using (SqlDataReader r = GetMessageBatch(queueName, con, timeout,1))
      {
        if (r == null || !r.HasRows  )
          return null;
        r.Read();
        Guid conversation_handle = r.GetGuid(r.GetOrdinal("conversation_handle"));
        string messageType = r.GetString(r.GetOrdinal("message_type_name"));
        if (messageType == "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog")
        {
          EndConversation(conversation_handle, con);
          return null;
        }
        var body = r.GetSqlBinary(r.GetOrdinal("message_body"));
        return body.Value;

      }
    }

    internal static void EndConversation(Guid conversationHandle,SqlConnection con)
    {
      
      try
      {
        string SQL = "END CONVERSATION @ConversationHandle;";

        using (SqlCommand cmd = new SqlCommand(SQL, con))
        {
          SqlParameter pConversation = cmd.Parameters.Add("@ConversationHandle", SqlDbType.UniqueIdentifier);
          pConversation.Value = conversationHandle;
          cmd.ExecuteNonQuery();
        }
      }
      catch (SqlException ex)
      {
        throw;
      }
    }

    /// This is the method that actually receives Service Broker messages.
    static SqlDataReader GetMessageBatch(string queueName, SqlConnection con, TimeSpan timeout, int maxMessages)
    {
      string SQL = string.Format(@"
            waitfor( 
                RECEIVE top (@count) conversation_handle,service_name,message_type_name,message_body,message_sequence_number 
                FROM [{0}] 
                    ), timeout @timeout", queueName);
      SqlCommand cmd = new SqlCommand(SQL, con);

      SqlParameter pCount = cmd.Parameters.Add("@count", SqlDbType.Int);
      pCount.Value = maxMessages;

      SqlParameter pTimeout = cmd.Parameters.Add("@timeout", SqlDbType.Int);

      if (timeout == TimeSpan.MaxValue)
      {
        pTimeout.Value = -1;
      }
      else
      {
        pTimeout.Value = (int)timeout.TotalMilliseconds;
      }

      cmd.CommandTimeout = 0; //honor the RECIEVE timeout, whatever it is.


      return cmd.ExecuteReader();
    }


  }
}
