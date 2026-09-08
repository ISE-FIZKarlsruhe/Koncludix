@echo off
rem setup.bat -- one-time setup for the Turtle/RDF <-> OWL2-XML converters
rem used to check Turtle-format data (like InferTest's test cases) against
rem the Windows Konclude build, which only reads/writes OWL2-XML/Functional
rem syntax natively (see ../README.md).
rem
rem Compiles ConvertToOWLXML.java/ConvertToRDFXML.java against the bundled
rem openllet.jar (a complete, self-contained OWL API build -- Maven
rem Central's own "distribution" jars for both OWL API and openllet were
rem tried first and found to be missing several of their own transitive
rem dependencies, cascading through slf4j, javax.inject, and caffeine
rem without ever fully resolving; this bundled jar is the one actually
rem verified to work end-to-end).
rem
rem Requires: a JDK on PATH (javac/java). No internet access needed --
rem everything required is already in this folder.

setlocal
set SCRIPT_DIR=%~dp0
set OPENLLET_JAR=%SCRIPT_DIR%openllet.jar
set CP=%OPENLLET_JAR%

where javac >nul 2>&1
if errorlevel 1 (
    echo ERROR: javac not found on PATH. Install a JDK ^(17+ recommended^) first.
    exit /b 1
)

if not exist "%OPENLLET_JAR%" (
    echo ERROR: %OPENLLET_JAR% not found -- it should be committed in this folder.
    exit /b 1
)

echo Compiling converters...
javac -cp "%CP%" "%SCRIPT_DIR%ConvertToOWLXML.java" "%SCRIPT_DIR%ConvertToRDFXML.java"
if errorlevel 1 (
    echo ERROR: compilation failed.
    exit /b 1
)

echo.
echo Done. Usage:
echo   java -cp "%CP%;%SCRIPT_DIR%" ConvertToOWLXML  input.ttl        output.owl.xml
echo   java -cp "%CP%;%SCRIPT_DIR%" ConvertToRDFXML  input.owl.xml    output.rdf.xml
