// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// #define LOG

namespace SessionSendReceiveUsingSessionClient
{
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class Program
    {
        // Connection String for the namespace can be obtained from the Azure portal under the 
        // 'Shared Access policies' section.
        static string ServiceBusConnectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONNECTION_STRING");
        const string QueueName = "sessionqueue";
        static IMessageSender messageSender;
        static ISessionClient sessionClient;
        const string SessionPrefix = "session-prefix";

        static void Main(string[] args)
        {
            if (args.Any(p => p == "fork"))
                RunFork();

            MainAsync(args.FirstOrDefault()).GetAwaiter().GetResult();
        }

        private static void RunFork()
        {
            var send = Run("send");
            var receive = Run("receive");

            send.WaitForExit();
            receive.WaitForExit();

            Process Run(string direction)
            {
                var process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = Process.GetCurrentProcess().MainModule.FileName,
                        Arguments = $"exec \"{typeof(Program).Assembly.Location}\" {direction}",
                        UseShellExecute = false,
                        WorkingDirectory = Environment.CurrentDirectory,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true
                    }
                };

                process.OutputDataReceived += (s, e) => Console.WriteLine($"[{direction}] {e.Data}");
                process.ErrorDataReceived += (s, e) => Console.Error.WriteLine($"[{direction}] {e.Data}");

                process.Start();

                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

                return process;
            }
        }

        static async Task MainAsync(string direction)
        {
            const int numberOfSessions = 1;
            const int numberOfMessagesPerSession = 200;

            if (direction == null || direction == "send")
                messageSender = new MessageSender(ServiceBusConnectionString, QueueName);
            if (direction == null || direction == "receive")
                sessionClient = new SessionClient(ServiceBusConnectionString, QueueName);

            if (direction == null)
            {
                await Task.WhenAll(
                    // Send messages with sessionId set
                    SendSessionMessagesAsync(numberOfSessions, numberOfMessagesPerSession),
                    // Receive all Session based messages using SessionClient
                    ReceiveSessionMessagesAsync(numberOfSessions, numberOfMessagesPerSession)
                );
            }
            else if (direction == "send")
            {
                await SendSessionMessagesAsync(numberOfSessions, numberOfMessagesPerSession);
            }
            else if (direction == "receive")
            {
                await ReceiveSessionMessagesAsync(numberOfSessions, numberOfMessagesPerSession);
            }


#if LOG
            Console.WriteLine("=========================================================");
            Console.WriteLine("Completed Receiving all messages... Press any key to exit");
            Console.WriteLine("=========================================================");
#endif

            //Console.ReadKey();

            if (messageSender != null)
                await messageSender.CloseAsync();
            if (sessionClient != null)
                await sessionClient.CloseAsync();
        }

        static async Task ReceiveSessionMessagesAsync(int numberOfSessions, int messagesPerSession)
        {
#if LOG
            Console.WriteLine("===================================================================");
            Console.WriteLine("Accepting sessions in the reverse order of sends for demo purposes");
            Console.WriteLine("===================================================================");
#endif

            for (int i = 0; i < numberOfSessions; i++)
            {
                int messagesReceivedPerSession = 0;
                long lastSequenceNumber = 0;

                // AcceptMessageSessionAsync(i.ToString()) as below with session id as parameter will try to get a session with that sessionId.
                // AcceptMessageSessionAsync() without any messages will try to get any available session with messages associated with that session.
                IMessageSession session = await sessionClient.AcceptMessageSessionAsync(SessionPrefix + i.ToString());

                if(session != null)
                {
                    // Messages within a session will always arrive in order.
#if LOG
                    Console.WriteLine("=====================================");
                    Console.WriteLine($"Received Session: {session.SessionId}");
#endif

                    while (messagesReceivedPerSession < messagesPerSession)
                    {
                        var messages = (await session.ReceiveAsync(100))
                            .OrderBy(p => p.SystemProperties.SequenceNumber)
                            .ToList();

                        foreach (var message in messages)
                        {
                            if (lastSequenceNumber != 0 && message.SystemProperties.SequenceNumber != lastSequenceNumber + 1)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine($"Got sequence number {message.SystemProperties.SequenceNumber}, expected {lastSequenceNumber + 1}");
                                Console.ResetColor();
                            }
                            lastSequenceNumber = message.SystemProperties.SequenceNumber;

#if LOG
                            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
#endif

                            messagesReceivedPerSession++;
                        }

                        // Complete the message so that it is not received again.
                        // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
                        await session.CompleteAsync(messages.Select(p => p.SystemProperties.LockToken).ToList());
                    }

#if LOG
                    Console.WriteLine($"Received all messages for Session: {session.SessionId}");
                    Console.WriteLine("=====================================");
#endif

                    // Close the Session after receiving all messages from the session
                    await session.CloseAsync();
                }
            }
        }

        static async Task SendSessionMessagesAsync(int numberOfSessions, int messagesPerSession)
        {      
            if (numberOfSessions == 0 || messagesPerSession == 0)
            {
                await Task.FromResult(false);
            }

            for (int i = numberOfSessions - 1; i >= 0; i--)
            {
                string sessionId = SessionPrefix + i;
                for (int j = 0; j < messagesPerSession; j++)
                {
                    // Create a new message to send to the queue
                    string messageBody = "test" + j;
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                    // Assign a SessionId for the message
                    message.SessionId = sessionId;

                    // Write the sessionId, body of the message to the console
#if LOG
                    Console.WriteLine($"Sending SessionId: {message.SessionId}, message: {messageBody}");
#endif

                    await messageSender.SendAsync(message);
                }
            }

#if LOG
            Console.WriteLine("=====================================");
            Console.WriteLine($"Sent {messagesPerSession} messages each for {numberOfSessions} sessions.");
            Console.WriteLine("=====================================");
#endif
        }
    }
}