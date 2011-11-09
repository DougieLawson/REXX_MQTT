#!/usr/bin/rxsock
/* rexx */
/*

See http://www.ibm.com/developerworks/webservices/library/ws-mqtt/index.html for the full protocol spec
 
 (C) 2011 Dougie Lawson, all rights reserved. 

*/

call RxFuncAdd 'SockLoadFuncs', 'rxsock', 'SockLoadFuncs'
call SockLoadFuncs

parse arg host port 

if port = '' then port = 1883
if host = '' then host = 'localhost'

keepalive = 1 /* second */

id = 'REXX pub client'
qos = 1
rc = mqtt_connect(host, port, id, keepalive, qos)
if rc <> 0 then do
     say "connect failed:" rc
     exit 20
     call SockClose s;
end

topic = 'examples/publishtest'
content = "Hello World" date() time()
/* content = content || copies('*',117400) || content */
say content 
rc = mqtt_publish(topic,content,qos)

call SockClose s;
exit 0 

mqtt_publish: procedure expose s

   topic = arg(1)
   content = arg(2)
   qos = arg(3)

   msgid = random()
   msgid = x2c(d2x(msgid,4))
   qos = qos * 2 /* shift 1 bit left */
   qos1 = x2c(d2x(qos,2))
   
   lth = hexlth(topic,4)
   if qos < 1 then buf = lth || topic || content
   else buf = lth || topic || msgid || content

   hlth = mqtt_length_encode(length(buf))
   
   pubcode = bitor('30'x,qos1)

   /*     PUBLISH      encoded len */
   header = pubcode || hlth
   msg = header || buf

/*
say "---->"
say msg
say c2x(msg)
*/

   if SockSend(s, msg) < 0 then do
      say 'Send() failed'
      say "SockErrno =" SockSock_Errno()
      exit 301
   end

   if qos > 1 then do
      if SockRecv(s, 'rsp1', 5) < 0 then do
         say 'Recv() failed'
         say "SockErrno =" SockSock_Errno()
         exit 302
      end

/*
say "<----"
say rsp1
say c2x(rsp1)
*/

      rsp2 = ""
      if length(rsp1) > 0 then do
         parse var rsp1 1 verb 2 rlth
         lth = mqtt_length_decode(rlth)
         parse var lth rlth offset
         rlth = rlth + 2 

         if SockRecv(s, 'rsp2', rlth) < 0 then do
            say 'Recv() failed'
            say "SockErrno =" SockSock_Errno()
            exit 303
         end

/*
say "<----"
say rsp2
say c2x(rsp2)
*/

         rsp = rsp1 || rsp2

         parse var rsp 1 verb 2 lth2 =(offset) rest
         say "Msgid:" c2x(rest)
      end
   end

   return 0

mqtt_connect: procedure expose s 

/* set up the MQTT CONNECT  and check the CONNACK */

   host = arg(1)
   port = arg(2)
   id = arg(3)
   keepalive = arg(4)
   qos = arg(5)
   
   qos = qos * 2
   qos = x2c(d2x(qos,2))

   s = tcp_connect(host, port)
   ka = x2c(d2x(keepalive,4))

   lth = hexlth(id,4)

   clientid = lth || id

   /* Set up connect struct. */
   /*     ll        protocol   version   conflags   */
   /*                                    clean      */
   buf = '0006'x || 'MQIsdp' || '03'x || '02'x ||  ka || clientid

   hlth = mqtt_length_encode(length(buf)) 
   /*      CONNECT   1-byte total len */ 
   concode = bitor('10'x,qos)
   header = concode || hlth

   msg = header || buf

/*
say "---->"
say msg
say c2x(msg)
*/

   if SockSend(s, msg) < 0 then do
      say 'Send() failed'
      say "SockErrno =" SockSock_Errno()
      exit 201
   end

   if SockRecv(s, 'rsp', 5, 'MSG_PEEK') < 0 then do
      say 'Recv() failed'
      say "SockErrno =" SockSock_Errno()
      exit 202
   end

/*
say "<----"
say rsp
say c2x(rsp)
*/

   parse var rsp 1 verb 2 rlth

   lth = mqtt_length_decode(rlth)
   parse var lth rlth offset
   rlth = rlth + 2

   if rlth > 0 then do
      if SockRecv(s, 'rsp', rlth) < 0 then do
         say 'Recv() failed'
         say "SockErrno =" SockSock_Errno()
         exit 203
      end
   end

/*
say "<----"
say rsp
say c2x(rsp)
*/
   parse var rsp 1 verb 2 lth =(offset) rsn
   rsn = right(rsn,1)

   if c2x(rsn) <> '00' then do
      say "verb:" c2x(verb)
      say "lth:" c2x(lth)
      say "rsn:" c2x(rsn)
      return 204
   end
   else return 0

tcp_connect: procedure
   host = arg(1)
   port = arg(2)

   if SockGetHostByName(host, 'host.!') = 0 then do
      say 'SockGetHostByName failed' sockpsock_errno()
      say 'Errno' SockSock_Errno();
      exit 101
   end

   /* Put server info into the server stem. */
   server.!family = 'AF_INET'
   server.!port   = port
   server.!addr   = host.!addr

   /* Get a stream socket. */
   s = SockSocket('AF_INET', 'SOCK_STREAM', 0)
   if s < 0 then do
      call SockPSock_Errno 'Socket'
      exit 102
   end

   /* Connect to the server. */
   if SockConnect(s, 'server.!') < 0 then do
      call SockPSock_Errno
      exit 103
   end

   return s

hexlth: procedure
   id = arg(1)
   rlth = arg(2)
   return x2c(d2x(length(id),rlth))

mqtt_length_encode: procedure
 
   x = arg(1)
   remlen = "" 
   do while(x > 0) 
 
      digit = x // 128
      x = x % 128
 
      if x > 0 then digit = digit + 128
      remlen = remlen || d2x(digit,2)

   end

   return x2c(remlen)

mqtt_length_decode: procedure
 
   remlen = arg(1)
   digits = c2x(remlen)

   multiplier = 1
   value = 0
   done = 0
   offset = 3
   do i = 1 to length(digits) by 2

      digit = x2d(substr(digits,i,2))
      if digit > 128 then digit = digit - 128
      else done = 1
      value = value + (digit * multiplier)
      if done = 1 then leave
      multiplier = multiplier * 128
      offset = offset + 1
   end
   ret = value offset
   return ret

