#!/usr/bin/env python

import os
import sys

if len(sys.argv) < 2:
   print("Find files in current directory")
   print("Usage: f ([directory]) [reg-exp] ([reg-exp] ..)")
   sys.exit("")

if "-name" in sys.argv:
   sys.argv.remove("-name")

directory = "."
if os.path.isdir(sys.argv[1]):
   directory = sys.argv[1]
   del sys.argv[1]

for regexp in sys.argv[1:]:
    cmd = 'find %(directory)s -name "%(regexp)s"' % locals()
    #print(cmd)
    os.system(cmd)
