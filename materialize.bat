@echo off
setlocal enabledelayedexpansion
rem materialize.bat -- Windows wrapper for our modified Konclude's "materialize"
rem command. Includes every fix from this session's work:
rem   - object property assertions (sub-property roll-up, inverse propagation,
rem     property-chain composition)
rem   - data property assertions
rem   - same-individual equivalences (functional/inverse-functional properties,
rem     cardinality, qualified cardinality)
rem   - correct handling of facts that pass through a blank node (anonymous
rem     individual), even when the final derived fact only involves named
rem     individuals
rem
rem Known, separately-tracked gaps in Konclude's own reasoning (not fixed by
rem anything here): owl:hasKey with a datatype-property key, and
rem owl:AsymmetricProperty / owl:IrreflexiveProperty / owl:propertyDisjointWith
rem violations are not detected. See this repo's README for details.
rem
rem This binary reads/writes OWL2-XML or OWL2-Functional syntax natively (NOT
rem Turtle/RDF-XML directly -- that native Turtle in/out support is Linux-only,
rem via the Docker image being finalized separately).
rem
rem Usage:
rem   materialize.bat input-file output-file
rem   materialize.bat input-file output-file -anon     (also write blank
rem                                                      nodes' own facts)
rem   materialize.bat -selftest                         (quick, ~1s sanity
rem                                                      check -- one
rem                                                      hardcoded case)
rem   materialize.bat -infertest                        (thorough: every
rem                                                      InferTest construct,
rem                                                      ~a few seconds --
rem                                                      see tools\run-infertest.ps1)

set SCRIPT_DIR=%~dp0
set KONCLUDE=%SCRIPT_DIR%Binaries\Konclude.exe

if "%~1"=="-selftest" goto selftest
if "%~1"=="-infertest" goto infertest
if "%~1"=="" goto usage
if "%~2"=="" goto usage

set INPUT=%~1
set OUTPUT=%~2
set EXTRA_ARGS=

if /i "%~3"=="-anon" (
    set EXTRA_ARGS=-c "%SCRIPT_DIR%Configs\anon-config.xml"
)

echo Running: Konclude materialize -w AUTO !EXTRA_ARGS! -i "%INPUT%" -o "%OUTPUT%"
"%KONCLUDE%" materialize -w AUTO !EXTRA_ARGS! -i "%INPUT%" -o "%OUTPUT%"
if errorlevel 1 (
    echo.
    echo FAILED -- see Konclude's own error output above.
    exit /b 1
)
echo.
echo Done. Materialized ontology written to "%OUTPUT%".
exit /b 0

:selftest
echo Running a quick self-test (blank-node property-chain case)...
set TEST_INPUT=%TEMP%\konclude-selftest.ofn
set TEST_OUTPUT=%TEMP%\konclude-selftest-out.owl.xml
(
echo Prefix(:=^<http://example.org/infertest#^>^)
echo Prefix(owl:=^<http://www.w3.org/2002/07/owl#^>^)
echo Ontology(^<http://example.org/infertest/selftest^>
echo     Declaration(ObjectProperty(:blankNodeLinksTo^)^)
echo     Declaration(ObjectProperty(:blankNodeConnectedTo^)^)
echo     Declaration(NamedIndividual(:Alpha^)^)
echo     Declaration(NamedIndividual(:Omega^)^)
echo     SubObjectPropertyOf(ObjectPropertyChain(:blankNodeLinksTo :blankNodeLinksTo^) :blankNodeConnectedTo^)
echo     ObjectPropertyAssertion(:blankNodeLinksTo :Alpha _:b1^)
echo     ObjectPropertyAssertion(:blankNodeLinksTo _:b1 :Omega^)
echo ^)
) > "%TEST_INPUT%"
"%KONCLUDE%" materialize -w AUTO -i "%TEST_INPUT%" -o "%TEST_OUTPUT%" >nul 2>&1
findstr /C:"blankNodeConnectedTo" "%TEST_OUTPUT%" >nul
if errorlevel 1 (
    echo SELF-TEST FAILED: expected entailment not found in "%TEST_OUTPUT%".
    exit /b 1
)
echo SELF-TEST PASSED: Konclude.exe is working correctly.
del "%TEST_INPUT%" "%TEST_OUTPUT%" >nul 2>&1
exit /b 0

:infertest
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%tools\run-infertest.ps1"
exit /b %errorlevel%

:usage
echo Usage: materialize.bat input-file output-file [-anon]
echo        materialize.bat -selftest
echo        materialize.bat -infertest
echo.
echo   input-file   OWL2-XML or OWL2-Functional-syntax ontology (.owl.xml, .ofn)
echo   output-file  Where to write the materialized ABox (same format)
echo   -anon        Also write anonymous individuals' own facts (off by default)
exit /b 1
