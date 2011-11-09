#!/usr/bin/rxsock

/*
 This shows how to encode/decode the remaining length bytes
 
 (C) 2011 Dougie Lawson, all rights reserved.
 */

 len_1 = '7d'x
 len_2 = 'de6d'x 
 len_3 = 'dead5d'x
 len_4 = 'deaded4d'x
 len_9 = 'de6d5555'x
 len_a = '7d5050'x

 say mqtt_length_decode(len_1)
 say mqtt_length_decode(len_2)
 say mqtt_length_decode(len_3)
 say mqtt_length_decode(len_4)
 say mqtt_length_decode(len_9)
 say mqtt_length_decode(len_a)

 len_5 = 125
 len_6 = 14046
 len_7 = 1529566
 len_8 = 163272414

 say c2x(mqtt_length_encode(len_5))
 say c2x(mqtt_length_encode(len_6))
 say c2x(mqtt_length_encode(len_7))
 say c2x(mqtt_length_encode(len_8))

exit 0

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

