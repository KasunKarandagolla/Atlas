; Per-user install. Runtime state lives outside {app}; uninstall keeps evidence.
#ifndef ProductVersion
  #error ProductVersion must be supplied by the clean build
#endif
#ifndef SourceSHA
  #error SourceSHA must be supplied by the clean build
#endif
#ifndef PayloadDir
  #error PayloadDir must be supplied by the clean build
#endif
#ifndef OutputDir
  #error OutputDir must be supplied by the clean build
#endif
[Setup]
AppId={{7D7E9B64-241F-487A-8536-9D5AD35554B1}
AppName=ATLAS
AppVersion={#ProductVersion}
AppVerName=ATLAS {#ProductVersion}
AppPublisher=ATLAS Research
VersionInfoVersion={#ProductVersion}
VersionInfoDescription=ATLAS public research desktop
VersionInfoProductName=ATLAS
DefaultDirName={localappdata}\Programs\ATLAS
DefaultGroupName=ATLAS
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0.22000
OutputDir={#OutputDir}
OutputBaseFilename=ATLAS-{#ProductVersion}-{#SourceSHA}-win11-x64-setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
DisableProgramGroupPage=yes
CloseApplications=yes
RestartApplications=no
UninstallDisplayIcon={app}\atlas-product.exe
SetupLogging=yes
#ifdef SignTool
SignTool={#SignTool}
SignedUninstaller=yes
#endif
[Files]
Source: "{#PayloadDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
[InstallDelete]
; Replace the complete frozen dependency tree: obsolete DLLs must not survive
; an upgrade and become an undeclared dependency. User data is outside {app}.
Type: filesandordirs; Name: "{app}\_internal"
[Icons]
Name: "{userprograms}\ATLAS"; Filename: "{app}\atlas-product.exe"; WorkingDir: "{app}"
[Run]
Filename: "{app}\atlas-product.exe"; Description: "Launch ATLAS"; Flags: nowait postinstall skipifsilent
; No [UninstallDelete]: configuration, credentials and evidence are never deleted.
