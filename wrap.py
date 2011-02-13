#!/usr/bin/env python
#################################################################################################
# Copyright (c) 2010, Lawrence Livermore National Security, LLC.  
# Produced at the Lawrence Livermore National Laboratory  
# Written by Todd Gamblin, tgamblin@llnl.gov.
# LLNL-CODE-417602
# All rights reserved.  
# 
# This file is part of Libra. For details, see http://github.com/tgamblin/libra.
# Please also read the LICENSE file for further information.
# 
# Redistribution and use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
# 
#  * Redistributions of source code must retain the above copyright notice, this list of
#    conditions and the disclaimer below.
#  * Redistributions in binary form must reproduce the above copyright notice, this list of
#    conditions and the disclaimer (as noted below) in the documentation and/or other materials
#    provided with the distribution.
#  * Neither the name of the LLNS/LLNL nor the names of its contributors may be used to endorse
#    or promote products derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
# OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# LAWRENCE LIVERMORE NATIONAL SECURITY, LLC, THE U.S. DEPARTMENT OF ENERGY OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#################################################################################################
usage_string = \
'''Usage: wrap.py [-fgd] [-i pmpi_init] [-c mpicc_name] [-o file] wrapper.w [...]
 Python script for creating PMPI wrappers. Roughly follows the syntax of 
   the Argonne PMPI wrapper generator, with some enhancements.
 Options:"
   -d             Just dump function declarations parsed out of mpi.h
   -f             Generate fortran wrappers in addition to C wrappers.
   -g             Generate reentry guards around wrapper functions.
   -c exe         Provide name of MPI compiler (for parsing mpi.h).  Default is \'mpicc\'.
   -i pmpi_init   Specify proper binding for the fortran pmpi_init function.
                  Default is \'pmpi_init_\'.  Wrappers compiled for PIC will guess the
                  right binding automatically (use -DPIC when you compile dynamic libs).
   -o file        Send output to a file instead of stdout.
   
 by Todd Gamblin, tgamblin@llnl.gov
'''
import tempfile, getopt, subprocess, sys, re, StringIO


# Default values for command-line parameters
mpicc = 'mpicc'                    # Default name for the MPI compiler
pmpi_init_binding = "pmpi_init_"   # Default binding for pmpi_init
output_fortran_wrappers = False    # Don't print fortran wrappers by default
output_guards = False              # Don't print reentry guards by default
dump_prototypes = False            # Just exit and dump MPI protos if false.

# Possible legal bindings for the fortran version of PMPI_Init()
pmpi_init_bindings = ["PMPI_INIT", "pmpi_init", "pmpi_init_", "pmpi_init__"]

# Possible function return types to consider, used for declaration parser.
# In general, all MPI calls we care about return int.  We include double
# to grab MPI_Wtick and MPI_Wtime, but we'll ignore the f2c and c2f calls 
# that return MPI_Datatypes and other such things.
rtypes = ['int', 'double' ]

# If we find these strings in a declaration, exclude it from consideration.
exclude_strings = [ "c2f", "f2c" ]

# Regular expressions for start and end of declarations in mpi.h. These are
# used to get the declaration strings out for parsing with formal_re below.
begin_decl_re = re.compile("(" + "|".join(rtypes) + ")\s+(MPI_\w+)\s*\(")
exclude_re =    re.compile("|".join(exclude_strings))
end_decl_re =   re.compile("\).*\;")

# Regular Expression for splitting up args. Matching against this 
# returns three groups: type info, arg name, and array info
formal_re = re.compile(
    "\s*(" +                       # Start type
    "(?:const)?\s*" +              # Initial const
    "\w+"                          # Type name (note: doesn't handle 'long long', etc. right now)
    ")\s*(" +                      # End type, begin pointers
    "(?:\s*\*(?:\s*const)?)*" +    # Look for 0 or more pointers with optional 'const'
    ")\s*"                         # End pointers
    "(?:(\w+)\s*)?" +              # Argument name. Optional.
     "(\[.*\])?\s*$"               # Array type.  Also optional. Works for multidimensions b/c it's greedy.
    )

# Fortran wrapper suffix
f_wrap_suffix = "_fortran_wrapper"

# Initial includes and defines for wrapper files.
wrapper_includes = '''
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef _EXTERN_C_
#ifdef __cplusplus
#define _EXTERN_C_ extern "C"
#else /* __cplusplus */
#define _EXTERN_C_ 
#endif /* __cplusplus */
#endif /* _EXTERN_C_ */

#ifdef MPICH_HAS_C2F
_EXTERN_C_ void *MPIR_ToPointer(int);
#endif // MPICH_HAS_C2F

#ifdef PIC
/* For shared libraries, declare these weak and figure out which one was linked
   based on which init wrapper was called.  See mpi_init wrappers.  */
#pragma weak pmpi_init
#pragma weak PMPI_INIT
#pragma weak pmpi_init_
#pragma weak pmpi_init__
#endif /* PIC */

_EXTERN_C_ void pmpi_init(MPI_Fint *ierr);
_EXTERN_C_ void PMPI_INIT(MPI_Fint *ierr);
_EXTERN_C_ void pmpi_init_(MPI_Fint *ierr);
_EXTERN_C_ void pmpi_init__(MPI_Fint *ierr);

'''

# Default modifiers for generated bindings
default_modifiers = ["_EXTERN_C_"]  # _EXTERN_C_ is #defined (or not) in wrapper_includes. See above.

# Set of MPI Handle types
mpi_handle_types = set(["MPI_Comm", "MPI_Errhandler", "MPI_File", "MPI_Group", "MPI_Info", 
                        "MPI_Op", "MPI_Request", "MPI_Status", "MPI_Datatype", "MPI_Win" ])

# MPI Calls that have array parameters, and mappings from the array parameter positions to the position
# of the 'count' paramters that determine their size
mpi_array_calls = {
    "MPI_Startall"           : { 1:0 },
    "MPI_Testall"            : { 1:0, 3:0 },
    "MPI_Testany"            : { 1:0 },
    "MPI_Testsome"           : { 1:0, 4:0 },
    "MPI_Type_create_struct" : { 3:0 },
    "MPI_Type_get_contents"  : { 6:1 },
    "MPI_Type_struct"        : { 3:0 },
    "MPI_Waitall"            : { 1:0, 2:0 },
    "MPI_Waitany"            : { 1:0 },
    "MPI_Waitsome"           : { 1:0, 4:0 }
}


def write_fortran_init_flag():
    output.write("static int fortran_init = 0;\n")

def once(function):
    if not hasattr(function, "did_once"):
        function()
        function.did_once = True

# Returns MPI_Blah_[f2c,c2f] prefix for a handle type
def conversion_prefix(handle_type):
    if handle_type == "MPI_Datatype":
        return "MPI_Type"
    else:
        return handle_type

# Special join function for joining lines together.  Puts "\n" at the end too.
def joinlines(list, sep="\n"):
    if list:
        return sep.join(list) + sep
    else:
        return ""

# Possible types of Tokens in input.
LBRACE, RBRACE, TEXT = range(3)

class Token:
    """Represents tokens; generated from input by Lexer and fed to parse()."""
    def __init__(self, type, value):
        self.type = type    # Type of token
        self.value = value  # Text value

    def isa(self, type):
        return self.type == type

class Lexer:
    """Lexes a wrapper file and spits out Tokens in order."""
    def __init__(self, lbrace, rbrace):
        self.lbrace = lbrace
        self.rbrace = rbrace
        self.in_tag = False
        self.text = StringIO.StringIO()
        self.line_no = 0
    
    def lex_line(self, line):
        length = len(line)
        start = 0

        while (start < length):
            if self.in_tag:
                brace_type, brace = (RBRACE, self.rbrace)
            else:
                brace_type, brace = (LBRACE, self.lbrace)

            end = line.find(brace, start)
            if (end >= 0):
                self.text.write(line[start:end])
                yield Token(TEXT, self.text.getvalue())
                yield Token(brace_type, brace)
                self.text.close()
                self.text = StringIO.StringIO()
                start = end + len(brace)
                self.in_tag = not self.in_tag
            else:
                self.text.write(line[start:])
                start = length

    def lex(self, file):
        self.line_no = 0
        for line in file:
            self.line_no += 1
            for token in self.lex_line(line):
                yield token

        # Yield last token if there's anything there.
        last = self.text.getvalue()
        if last:
            yield Token(TEXT, last)

# Lexer for wrapper files used by parse() routine below.
lexer = Lexer("{{","}}")

# Global current filename for error msgs
filename = ""

def syntax_error(msg):
    print "%s:%d: %s" % (filename, lexer.line_no, msg)
    sys.exit(1)
    
# Map from function name to declaration created from mpi.h.
mpi_functions = {}

# Global table of macro functions, keyed by name.
macros = {}

class Param:
    """Descriptor for formal parameters of MPI functions.  
       Doesn't represent a full parse, only the initial type information, 
       name, and array info of the argument split up into strings.
    """
    def __init__(self, type, pointers, name, array, pos):
        self.type = type               # Name of arg's type (might include things like 'const')
        self.pointers = pointers       # Pointers
        self.name = name               # Formal parameter name (from header or autogenerated)
        self.array = array             # Any array type information after the name
        self.pos = pos                 # Position of arg in declartion
        self.decl = None               # This gets set later by Declaration

    def setDeclaration(self, decl):
        """Needs to be called by Declaration to finish initing the arg."""
        self.decl = decl
        
    def isHandleArray(self):
        """True if this Param represents an array of MPI handle values."""
        return (self.decl.name in mpi_array_calls
                and self.pos in mpi_array_calls[self.decl.name])

    def countParam(self):
        """If this Param is a handle array, returns the Param that represents the count of its elements"""
        return self.decl.args[mpi_array_calls[self.decl.name][self.pos]]
    
    def isHandle(self):
        """True if this Param is one of the MPI builtin handle types."""
        return self.type in mpi_handle_types

    def isStatus(self):
        """True if this Param is an MPI_Status.  MPI_Status is handled differently 
           in c2f/f2c calls from the other handle types.
        """
        return self.type == "MPI_Status"

    def fortranFormal(self):
        """Prints out a formal parameter for a fortran wrapper."""
        # There are only a few possible fortran arg types in our wrappers, since
        # everything is a pointer.
        if self.type == "MPI_Aint" or self.type.endswith("_function"):
            ftype = self.type
        else:
            ftype = "MPI_Fint"

        # Arrays don't come in as pointers (they're passed as arrays)
        # Everything else is a pointer.
        if self.pointers:
            pointers = self.pointers
        elif self.array:
            pointers = ""
        else:
            pointers = "*"
            
        # Put it all together and return the fortran wrapper type here.
        arr = self.array or ''
        return "%s %s%s%s" % (ftype, pointers, self.name, arr)

    def cFormal(self):
        """Prints out a formal parameter for a C wrapper."""
        if not self.type:
            return self.name  # special case for '...'
        else:
            arr = self.array or ''
            pointers = self.pointers or ''
            return "%s %s%s%s" % (self.type, pointers, self.name, arr)

    def castType(self):
        arr = self.array or ''
        pointers = self.pointers or ''
        if '[]' in arr:
            if arr.count('[') > 1:
                pointers += '(*)'   # need extra parens for, e.g., int[][3] -> int(*)[3]
            else:
                pointers += '*'     # justa single array; can pass pointer.
            arr = arr.replace('[]', '')
        return "%s%s%s" % (self.type, pointers, arr)

    def __str__(self):
        return self.cFormal()


class Declaration:
    """ Descriptor for simple MPI function declarations.  
        Contains return type, name of function, and a list of args.
    """
    def __init__(self, rtype, name):
        self.rtype = rtype
        self.name = name
        self.args = []

    def addArgument(self, arg):
        arg.setDeclaration(self)
        self.args.append(arg)

    def __iter__(self):
        for arg in self.args: yield arg

    def __str__(self):
        return self.prototype()

    def retType(self):
        return self.rtype

    def argTypeList(self):
        return "(" + ", ".join(map(Param.cFormal, self.args)) + ")"

    def argsNoEllipsis(self):
        return filter(lambda arg: arg.name != "...", self.args)

    def fortranArgTypeList(self):
        formals = map(Param.fortranFormal, self.argsNoEllipsis())
        if self.name == "MPI_Init": formals = []
        return "(%s)" % ", ".join(formals + ["MPI_Fint *ierr"])

    def argList(self):
        names = [arg.name for arg in self.argsNoEllipsis()]
        return "(%s)" % ", ".join(names)

    def fortranArgList(self):
        names = [arg.name for arg in self.argsNoEllipsis()]
        if self.name == "MPI_Init": names = []
        return "(%s)" % ", ".join(names + ["ierr"])

    def prototype(self, modifiers=""):
        if modifiers: modifiers = joinlines(modifiers, " ")
        return "%s%s %s%s" % (modifiers, self.retType(), self.name, self.argTypeList())
    
    def pmpi_prototype(self, modifiers=""):
        if modifiers: modifiers = joinlines(modifiers, " ")
        return "%s%s P%s%s" % (modifiers, self.retType(), self.name, self.argTypeList())
        
    def fortranPrototype(self, name=None, modifiers=""):
        if not name: name = self.name
        if modifiers: modifiers = joinlines(modifiers, " ")
        return "%svoid %s%s" % (modifiers, name, self.fortranArgTypeList())
    

types = set()
all_pointers = set()

def enumerate_mpi_declarations(mpicc):
    """ Invokes mpicc's C preprocessor on a C file that includes mpi.h.
        Parses the output for declarations, and yields each declaration to
        the caller.
    """
    # Create an input file that just includes <mpi.h> 
    tmpfile = tempfile.NamedTemporaryFile('w+b', -1, '.c')
    tmpname = "%s" % tmpfile.name
    tmpfile.write('#include <mpi.h>')
    tmpfile.write("\n")
    tmpfile.flush()

    # Run the mpicc -E on the temp file and pipe the output 
    # back to this process for parsing.
    mpicc_cmd = "%s -E" % mpicc
    try:
        popen = subprocess.Popen("%s %s" % (mpicc_cmd, tmpname), shell=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except IOError:
        print "IOError: couldn't run '" + mpicc_cmd + "' for parsing mpi.h"
        sys.exit(1)
    
    # Parse out the declarations from the MPI file
    mpi_h = popen.stdout
    for line in mpi_h:
        line = line.strip()
        begin = begin_decl_re.search(line)
        if begin and not exclude_re.search(line):
            # Grab return type and fn name from initial parse
            return_type, fn_name = begin.groups()

            # Accumulate rest of declaration (possibly multi-line)
            while not end_decl_re.search(line):
                line += " " + mpi_h.next().strip()

            # Split args up by commas so we can parse them independently
            arg_string = re.search(fn_name + "\s*\((.*)\)", line).group(1)
            arg_list = map(lambda s: s.strip(), arg_string.split(","))

            # Handle functions that take no args specially
            if arg_list == ['void']:
                arg_list = []
            
            # Parse formal parameter descriptors out of args            
            decl = Declaration(return_type, fn_name)
            arg_num = 0
            for arg in arg_list:
                if arg == '...':   # Special case for Pcontrol.
                    decl.addArgument(Param(None, None, '...', None, arg_num))
                else:
                    match = formal_re.match(arg)
                    if not match:
                        print "MATCH FAILED FOR: '" + arg + "' in " + fn_name
                        sys.exit(1)

                    type, pointers, name, array = match.groups()
                    types.add(type)
                    all_pointers.add(pointers)
                    # If there's no name, make one up.
                    if not name: name = "arg_" + str(arg_num)
                        
                    decl.addArgument(Param(type.strip(), pointers, name, array, arg_num))
                arg_num += 1
                    
            yield decl

    error_status = mpi_h.close()
    if (error_status):
        print "Error: Couldn't run '" + mpicc_cmd + "' for parsing mpi.h."
        print "       Process exited with code " + str(error_status)
        sys.exit(1)

    # Do some cleanup once we're done reading.
    tmpfile.close()


def write_enter_guard(out, decl):
    """Prevent us from entering wrapper functions if we're already in a wrapper function.
       Just call the PMPI function w/o the wrapper instead."""
    if output_guards:
        out.write("    if (in_wrapper) return P%s%s;\n" % (decl.name, decl.argList()))
        out.write("    in_wrapper = 1;\n")

def write_exit_guard(out):
    """After a call, set in_wrapper back to 0 so we can enter the next call."""
    if output_guards:
        out.write("    in_wrapper = 0;\n")


def write_c_wrapper(out, decl, return_val, write_body):
    """Write the C wrapper for an MPI function."""
    # Write the PMPI prototype here in case mpi.h doesn't define it
    # (sadly the case with some MPI implementaitons)
    out.write(decl.pmpi_prototype(default_modifiers))
    out.write(";\n")

    # Now write the wrapper function, which will call the PMPI function we declared.
    out.write(decl.prototype(default_modifiers))
    out.write(" { \n")
    out.write("    int %s = 0;\n" % return_val)

    write_enter_guard(out, decl)
    write_body(out)
    write_exit_guard(out)

    out.write("    return %s;\n" % return_val)
    out.write("}\n\n")


def write_fortran_binding(out, decl, delegate_name, binding, stmts=None):
    """Outputs a wrapper for a particular fortran binding that delegates to the
       primary Fortran wrapper.  Optionally takes a list of statements to execute
       before delegating.
    """
    out.write(decl.fortranPrototype(binding, default_modifiers))
    out.write(" { \n")
    if stmts:
        out.write(joinlines(map(lambda s: "    " + s, stmts)))
    out.write("    %s%s;\n" % (delegate_name, decl.fortranArgList()))
    out.write("}\n\n")
    

class FortranDelegation:
    """Class for constructing a call to a Fortran wrapper delegate function.  Provides
       storage for local temporary variables, copies of parameters, callsites for MPI-1 and
       MPI-2, and writebacks to local pointer types.
    """
    def __init__(self, fn_name, return_val):
        self.fn_name = fn_name
        self.return_val = return_val

        self.temps = set()
        self.copies = []
        self.writebacks = []
        self.actuals = []
        self.mpich_actuals = []

    def addTemp(self, type, name):
        """Adds a temp var with a particular name.  Adds the same var only once."""
        temp = "    %s %s;" % (type, name)
        self.temps.add(temp)

    def addActual(self, actual):
        self.actuals.append(actual)
        self.mpich_actuals.append(actual)
        
    def addActualMPICH(self, actual):
        self.mpich_actuals.append(actual)

    def addActualMPI2(self, actual):
        self.actuals.append(actual)

    def addWriteback(self, stmt):
        self.writebacks.append("    %s" % stmt)

    def addCopy(self, stmt):
        self.copies.append("    %s" % stmt)

    def write(self, out):
        assert len(self.actuals) == len(self.mpich_actuals)
        
        call = "    %s = %s" % (self.return_val, self.fn_name)
        mpich_call = "%s(%s);\n" % (call, ", ".join(self.mpich_actuals))
        mpi2_call = "%s(%s);\n" % (call, ", ".join(self.actuals))

        out.write("    int return_val = 0;\n")
        if mpich_call == mpi2_call and not (self.temps or self.copies or self.writebacks):
            out.write(mpich_call)
        else:
            out.write("#if (!defined(MPICH_HAS_C2F) && defined(MPICH_NAME) && (MPICH_NAME == 1)) /* MPICH test */\n")
            out.write(mpich_call)
            out.write("#else /* MPI-2 safe call */\n")
            out.write(joinlines(self.temps))
            out.write(joinlines(self.copies))
            out.write(mpi2_call)
            out.write(joinlines(self.writebacks))
            out.write("#endif /* MPICH test */\n")


def write_fortran_wrappers(out, decl, return_val):
    """Writes primary fortran wrapper that handles arg translation.
       Also outputs bindings for this wrapper for different types of fortran compilers.
    """
    delegate_name = decl.name + f_wrap_suffix
    out.write(decl.fortranPrototype(delegate_name, ["static"]))
    out.write(" { \n")

    call = FortranDelegation(decl.name, return_val)
    
    if decl.name == "MPI_Init":
        # Use out.write() here so it comes at very beginning of wrapper function
        out.write("    int argc = 0;\n");
        out.write("    char ** argv = NULL;\n");
        call.addActual("&argc");
        call.addActual("&argv");
        call.write(out)
        out.write("    *ierr = %s;\n" % return_val)
        out.write("}\n\n")
    
        # Write out various bindings that delegate to the main fortran wrapper
        write_fortran_binding(out, decl, delegate_name, "MPI_INIT",   ["fortran_init = 1;"])
        write_fortran_binding(out, decl, delegate_name, "mpi_init",   ["fortran_init = 2;"])
        write_fortran_binding(out, decl, delegate_name, "mpi_init_",  ["fortran_init = 3;"])
        write_fortran_binding(out, decl, delegate_name, "mpi_init__", ["fortran_init = 4;"])
        return

    # This look processes the rest of the call for all other routines.
    for arg in decl.args:
        if arg.name == "...":   # skip ellipsis
            continue
    
        if not (arg.pointers or arg.array):
            if not arg.isHandle():
                # These are pass-by-value arguments, so just deref and pass thru
                dereferenced = "*%s" % arg.name
                call.addActual(dereferenced)
            else:
                # Non-ptr, non-arr handles need to be converted with MPI_Blah_f2c
                # No special case for MPI_Status here because MPI_Statuses are never passed by value.
                call.addActualMPI2("%s_f2c(*%s)" % (conversion_prefix(arg.type), arg.name))
                call.addActualMPICH("(%s)(*%s)" % (arg.type, arg.name))

        else:
            if not arg.isHandle():
                # Non-MPI handle pointer types can be passed w/o dereferencing, but need to
                # cast to correct pointer type first (from MPI_Fint*).
                call.addActual("(%s)%s" % (arg.castType(), arg.name))
            else:
                # For MPI-1, assume ints, cross fingers, and pass things straight through.
                call.addActualMPICH("(%s*)%s" % (arg.type, arg.name))
                conv = conversion_prefix(arg.type)
                temp = "temp_%s" % arg.name

                # For MPI-2, other pointer and array types need temporaries and special conversions.
                if not arg.isHandleArray():
                    call.addTemp(arg.type, temp)
                    call.addActualMPI2("&%s" % temp)

                    if arg.isStatus():
                        call.addCopy("%s_f2c(%s, &%s);"  % (conv, arg.name, temp))
                        call.addWriteback("%s_c2f(&%s, %s);" % (conv, temp, arg.name))
                    else:
                        call.addCopy("%s = %s_f2c(*%s);"  % (temp, conv, arg.name))
                        call.addWriteback("*%s = %s_c2f(%s);" % (arg.name, conv, temp))
                else:
                    # Make temporary variables for the array and the loop var
                    temp_arr_type = "%s*" % arg.type
                    call.addTemp(temp_arr_type, temp)
                    call.addTemp("int", "i")
                
                    # generate a copy and a writeback statement for this type of handle
                    if arg.isStatus():
                        copy = "    %s_f2c(&%s[i], &%s[i])"  % (conv, arg.name, temp)
                        writeback = "    %s_c2f(&%s[i], &%s[i])" % (conv, temp, arg.name)
                    else:
                        copy = "    temp_%s[i] = %s_f2c(%s[i])"  % (arg.name, conv, arg.name)
                        writeback = "    %s[i] = %s_c2f(temp_%s[i])" % (arg.name, conv, arg.name)
                
                    # Generate the call surrounded by temp array allocation, copies, writebacks, and temp free
                    count = "*%s" % arg.countParam().name
                    call.addCopy("%s = (%s)malloc(sizeof(%s) * %s);" %
                                 (temp, temp_arr_type, arg.type, count))
                    call.addCopy("for (i=0; i < %s; i++)" % count)
                    call.addCopy("%s;" % copy)
                    call.addActualMPI2(temp)
                    call.addWriteback("for (i=0; i < %s; i++)" % count)
                    call.addWriteback("%s;" % writeback)
                    call.addWriteback("free(%s);" % temp)

    call.write(out)
    out.write("    *ierr = %s;\n" % return_val)
    out.write("}\n\n")

    # Write out various bindings that delegate to the main fortran wrapper
    write_fortran_binding(out, decl, delegate_name, decl.name.upper())
    write_fortran_binding(out, decl, delegate_name, decl.name.lower())
    write_fortran_binding(out, decl, delegate_name, decl.name.lower() + "_")
    write_fortran_binding(out, decl, delegate_name, decl.name.lower() + "__")


class Scope:
    """Maps string keys to either macros or values.
       Supports nesting: if values are not found in this scope, then 
       enclosing scopes are searched recursively.
    """
    def __init__(self, enclosing_scope=None):
        self.map = {}
        self.enclosing_scope = enclosing_scope

    def __getitem__(self, key):
        if key in self.map:
            return self.map[key]
        elif self.enclosing_scope:
            return self.enclosing_scope[key]
        else:
            raise KeyError(key + " is not in scope.")

    def __contains__(self, key):
        if key in self.map:
            return True
        elif self.enclosing_scope:
            return key in self.enclosing_scope
        else:
            return False

    def __setitem__(self, key, value):
        self.map[key] = value

    def include(self, map):
        """Add entire contents of the map (or scope) to this scope."""
        for key in map:
            self.map[key] = map[key]

    def include_decl(self, decl):
        self["retType"]     = decl.retType()
        self["argTypeList"] = decl.argTypeList()
        self["argList"]     = decl.argList()


def macro(fun):
    """Put a function in the macro table if it's annotated as a macro."""
    macros[fun.__name__] = fun
    return fun

def all_but(fn_list):
    """Return a list of all mpi functions except those in fn_list"""
    all_mpi = set(mpi_functions.keys())
    diff = all_mpi - set(fn_list)
    return [x for x in diff]

@macro
def foreachfn(out, scope, args, children):
    """Iterate over all functions listed in args."""
    args or syntax_error("Error: foreachfn requires function name argument.")
        
    fn_var = args[0]
    for fn_name in args[1:]:
        if not fn_name in mpi_functions:
            raise SyntaxError(fn_name + " is not an MPI function")

        fn = mpi_functions[fn_name]
        scope[fn_var] = fn_name
        scope.include_decl(fn)
        for child in children:
            child.execute(out, scope)

@macro
def fn(out, scope, args, children):
    """Iterate over listed functions and generate skeleton too."""
    args or syntax_error("Error: fn requires function name argument.")

    fn_var = args[0]
    for fn_name in args[1:]:
        if not fn_name in mpi_functions:
            raise SyntaxError(fn_name + " is not an MPI function")

        fn = mpi_functions[fn_name]
        return_val = "return_val"

        scope[fn_var] = fn_name
        scope.include_decl(fn)
        scope["return_val"] = return_val

        c_call = "%s = P%s%s;" % (return_val, fn.name, fn.argList())
        if fn_name == "MPI_Init" and output_fortran_wrappers:
            def callfn(out, scope, args, children):
                # All this is to deal with fortran, since fortran's MPI_Init() function is different
                # from C's.  We need to make sure to delegate specifically to the fortran init wrapping.
                # For dynamic libs, we use weak symbols to pick it automatically.  For static libs, need
                # to rely on input from the user via pmpi_init_binding and the -i option.
                out.write("    if (fortran_init) {\n")
                out.write("#ifdef PIC\n")
                out.write("        if (!PMPI_INIT && !pmpi_init && !pmpi_init_ && !pmpi_init__) {\n")
                out.write("            fprintf(stderr, \"ERROR: Couldn't find fortran pmpi_init function.  Link against static library instead.\\n\");\n")
                out.write("            exit(1);\n")
                out.write("        }")
                out.write("        switch (fortran_init) {\n")
                out.write("        case 1: PMPI_INIT(&return_val); break;\n")
                out.write("        case 2: pmpi_init(&return_val); break;\n")
                out.write("        case 3: pmpi_init_(&return_val); break;\n")
                out.write("        case 4: pmpi_init__(&return_val); break;\n")
                out.write("        default:\n")
                out.write("            fprintf(stderr, \"NO SUITABLE FORTRAN MPI_INIT BINDING\\n\");\n")
                out.write("            break;\n")
                out.write("        }\n")
                out.write("#else /* !PIC */\n")
                out.write("        %s(&return_val);\n" % pmpi_init_binding)
                out.write("#endif /* !PIC */\n")
                out.write("    } else {\n")
                out.write("        %s\n" % c_call)
                out.write("    }\n")

            scope["callfn"] = callfn
            once(write_fortran_init_flag)

        else:
            scope["callfn"] = c_call
            
        def write_body(out):
            for child in children:
                child.execute(out, scope)

        out.write("/* ================== C Wrappers for %s ================== */\n" % fn_name)
        write_c_wrapper(out, fn, return_val, write_body)
        if output_fortran_wrappers:
            out.write("/* =============== Fortran Wrappers for %s =============== */\n" % fn_name)
            write_fortran_wrappers(out, fn, return_val)
            out.write("/* ================= End Wrappers for %s ================= */\n\n\n" % fn_name)

@macro
def forallfn(out, scope, args, children):
    """Iterate over all but the functions listed in args."""
    args or syntax_error("Error: forallfn requires function name argument.")
    foreachfn(out, scope, [args[0]] + all_but(args[1:]), children)

@macro
def fnall(out, scope, args, children):
    """Iterate over all but listed functions and generate skeleton too."""
    args or syntax_error("Error: fnall requires function name argument.")
    fn(out, scope, [args[0]] + all_but(args[1:]), children)


class Chunk:
    """Represents a piece of a wrapper file.  Is either a text chunk
       or a macro chunk with children to which the macro should be applied.
       macros are evaluated lazily, so the macro is just a string until 
       execute is called and it is fetched from its enclosing scope."""
    def __init__(self):
        self.macro    = None
        self.args     = []
        self.text     = None
        self.children = []

    def iwrite(self, file, level, text):
        """Write indented text."""
        for x in xrange(level): 
            file.write("  ")
        file.write(text)

    def write(self, file=sys.stdout, l=0):
        if self.macro: 
            self.iwrite(file, l, self.macro + "\n")

        if self.args: 
            self.iwrite(file, l, " ".join(self.args) + "\n")

        if self.text: 
            self.iwrite(file, l, "TEXT\n")

        for child in self.children:
            child.write(file, l+1)

    def execute(self, out, scope):
        if not self.macro:
            out.write(self.text)
        else:
            if not self.macro in scope:
                raise SyntaxError("Invalid macro: " + self.macro)

            macro = scope[self.macro]
            if isinstance(macro, str):
                # raw strings in the scope will just get printed out.
                out.write(macro)
            else:
                # macros get executed inside a new scope 
                macro(out, Scope(scope), self.args, self.children)
        

def parse(tokens, macros, end_macro=None):
    """Turns a string of tokens into a list of chunks.
       Macros that have a has_body attribute will be recursively parsed
       and the result will be appended as a list of child chunks."""
    chunk_list = []

    for token in tokens:
        chunk = Chunk()

        if token.isa(TEXT):
            chunk.text = token.value

        elif token.isa(LBRACE):
            text, close = tokens.next(), tokens.next()
            if not text.isa(TEXT) or not close.isa(RBRACE):
                raise SyntaxError("Expected macro body after open brace.")

            args = text.value.split()
            name = args.pop(0)
            if name == end_macro:
                break
            else:
                chunk.macro = name
                chunk.args  = args
                if name in macros:
                    chunk.children = parse(tokens, macros, "end" + name)
        else:
            raise SyntaxError("Expected text block or macro.")

        chunk_list.append(chunk)

    return chunk_list


def usage():
    print usage_string
    sys.exit(2)

# Let the user specify another mpicc to get mpi.h from
output = sys.stdout
try:
    opts, args = getopt.gnu_getopt(sys.argv[1:], "fgdc:o:i:")
except getopt.GetoptError, err:
    print err
    usage()

for opt, arg in opts:
    if opt == "-d": 
        dump_prototypes = True
    if opt == "-f": 
        output_fortran_wrappers = True
    if opt == "-g": 
        output_guards = True
    if opt == "-c": 
        mpicc = arg
    if opt == "-i":
        if not arg in pmpi_init_bindings:
            print "ERROR: PMPI_Init binding must be one of:\n    %s\n" % " ".join(possible_bindings)
            usage()
        else:
            pmpi_init_binding = arg
    if opt == "-o": 
        try:
            output = open(arg, "w")
        except IOError:
            sys.stderr.write("Error: couldn't open file " + arg + " for writing.\n")
            sys.exit(1)

if len(args) < 1 and not dump_prototypes:
    usage()

#
# Parse mpi.h and put declarations into a map.
#
for decl in enumerate_mpi_declarations(mpicc):
    mpi_functions[decl.name] = decl
    if dump_prototypes:
        print decl

# If we're just dumping prototypes, we can just exit here.
if dump_prototypes:
    sys.exit(0)

# Start with some headers and definitions.
output.write(wrapper_includes)

if output_guards:
    output.write("static int in_wrapper = 0;\n")

#
# Parse each file listed on the command line and execute
# it once it's parsed.
#
fileno = 0
for f in args:
    filename = f
    file = open(filename)

    # Outer scope contains fileno and the basic macros.
    outer_scope = Scope()
    outer_scope["fileno"] = str(fileno)
    outer_scope.include(macros)

    chunks = parse(lexer.lex(file), macros)
    for chunk in chunks:
        chunk.execute(output, outer_scope)

    fileno += 1

output.close()
