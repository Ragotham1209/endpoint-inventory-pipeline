<#
.SYNOPSIS
    Gathers endpoint hardware, software, drivers, and custom inventory.
    Calculates independent hashes for each payload type.
    Sends payloads to the Azure Function if their specific hash differs.

.DESCRIPTION
    This script is configured for the Azure environment to route multiple payloads.
    A configurable startup jitter prevents fleet-wide API stampedes when deployed
    via Group Policy or SCCM at a fixed schedule across thousands of endpoints.
#>

param (
    # The URL of the deployed Azure Function (replace with your deployment URL)
    [string]$ApiUrl = "https://<YOUR-FUNCTION-APP>.azurewebsites.net/api/InventoryIngest",
    
    # The Function Key retrieved after deployment (replace with your key)
    [string]$ApiKey = "<YOUR-FUNCTION-KEY>",
    
    # Base path to store hashes locally
    [string]$HashDir = "C:\ProgramData\EndpointInventory",
    
    # Maximum random delay in seconds before execution begins.
    # Distributes API load when deployed fleet-wide via Group Policy.
    # Set to 0 to disable jitter (e.g., for interactive testing).
    [int]$MaxJitterSeconds = 300
)

# -------------------------------------------------------------------------
# 0. Startup Jitter (Fleet Stampede Prevention)
# -------------------------------------------------------------------------
$JitterApplied = 0
if ($MaxJitterSeconds -gt 0) {
    $JitterApplied = Get-Random -Minimum 0 -Maximum $MaxJitterSeconds
    Write-Output "Applying startup jitter: $JitterApplied seconds (max: $MaxJitterSeconds)."
    Start-Sleep -Seconds $JitterApplied
}

# -------------------------------------------------------------------------
# 1. Ensure Local Directory Exists and Send Start Audit
# -------------------------------------------------------------------------
$ExecutionStartTime = Get-Date
if (-not (Test-Path $HashDir)) {
    New-Item -ItemType Directory -Force -Path $HashDir | Out-Null
}

$EndpointId = $env:COMPUTERNAME

$script:HardwareStatus = "Pending"
$script:SoftwareStatus = "Pending"
$script:DriversStatus = "Pending"
$script:CustomStatus = "Pending"

$Headers = @{
    "Content-Type"    = "application/json"
    "x-functions-key" = $ApiKey
}

# Fire ExecutionStarted Audit event
$StartAuditPayload = @{
    PayloadType = "Audit"
    EndpointId  = $EndpointId
    Data        = @{ Event = "ExecutionStarted"; Status = "In Progress" }
}
try { Invoke-RestMethod -Uri $ApiUrl -Method Post -Headers $Headers -Body ($StartAuditPayload | ConvertTo-Json -Compress) | Out-Null } catch { }

# -------------------------------------------------------------------------
# Helper Function: Compute Hash, Compare, and Send
# -------------------------------------------------------------------------
function Send-InventoryIfChanged {
    param (
        [string]$PayloadType,
        [hashtable]$Data
    )

    $HashFile = Join-Path $HashDir "hash_$($PayloadType.ToLower()).txt"
    
    $FullPayload = @{
        PayloadType = $PayloadType
        EndpointId  = $EndpointId
        Data        = $Data
    }
    
    $JsonPayload = $FullPayload | ConvertTo-Json -Depth 5 -Compress
    
    # Compute Hash
    $Bytes = [System.Text.Encoding]::UTF8.GetBytes($JsonPayload)
    $Hasher = [System.Security.Cryptography.SHA256]::Create()
    $HashBytes = $Hasher.ComputeHash($Bytes)
    $CurrentHash = [BitConverter]::ToString($HashBytes) -replace '-'
    
    # Read Previous Hash
    $PreviousHash = ""
    if (Test-Path $HashFile) {
        $PreviousHash = Get-Content $HashFile
    }
    
    # Compare and Send
    if ($CurrentHash -ne $PreviousHash) {
        Write-Output "Change detected for $PayloadType payload. Sending to Azure API."
        
        $Headers = @{
            "Content-Type"    = "application/json"
            "x-functions-key" = $ApiKey
        }
        
        try {
            $Response = Invoke-WebRequest -Uri $ApiUrl -Method Post -Headers $Headers -Body $JsonPayload -UseBasicParsing
            Write-Output "Successfully sent $PayloadType payload. Local hash updated."
            Set-Content -Path $HashFile -Value $CurrentHash
            $StatusString = "Success (HTTP $($Response.StatusCode))"
        }
        catch {
            $ErrorMsg = $_.Exception.Message
            
            if ($_.Exception.Response) {
                $StatusCode = $_.Exception.Response.StatusCode.value__
                $StatusString = "Failed (HTTP $StatusCode)"
                $Reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $ResponseBody = $Reader.ReadToEnd()
                Write-Error "Failed to send $PayloadType payload. Error: HTTP $StatusCode - $ResponseBody"
            }
            else {
                $StatusString = "Failed (Network/System Error)"
                Write-Error "Failed to send $PayloadType payload. Error: $ErrorMsg"
            }
        }
    }
    else {
        Write-Output "No changes detected for $PayloadType payload. Skipping."
        $StatusString = "Skipped (No Change)"
    }
    
    if ($PayloadType -eq "Hardware") { $script:HardwareStatus = $StatusString }
    if ($PayloadType -eq "Software") { $script:SoftwareStatus = $StatusString }
    if ($PayloadType -eq "Drivers") { $script:DriversStatus = $StatusString }
    if ($PayloadType -eq "Custom") { $script:CustomStatus = $StatusString }
}

Write-Output "Starting inventory collection for $EndpointId..."

# -------------------------------------------------------------------------
# 2. Hardware Inventory
# -------------------------------------------------------------------------
Write-Output "Gathering Hardware Inventory..."
$OsInfo = Get-CimInstance -ClassName Win32_OperatingSystem | Select-Object Caption, Version, OSArchitecture
$CpuInfo = Get-CimInstance -ClassName Win32_Processor | Select-Object -First 1 Name
$TotalMemoryGB = [math]::Round(((Get-CimInstance -ClassName Win32_ComputerSystem).TotalPhysicalMemory / 1GB), 2)

$HardwareData = @{
    OSName         = $OsInfo.Caption
    OSVersion      = $OsInfo.Version
    OSArchitecture = $OsInfo.OSArchitecture
    CPUName        = $CpuInfo.Name
    TotalMemoryGB  = $TotalMemoryGB
}
Send-InventoryIfChanged -PayloadType "Hardware" -Data $HardwareData

# -------------------------------------------------------------------------
# 3. Software Inventory
# -------------------------------------------------------------------------
Write-Output "Gathering Software Inventory..."
$UninstallPaths = @(
    "HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*",
    "HKLM:\Software\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*"
)
$InstalledSoftware = @()
foreach ($Path in $UninstallPaths) {
    if (Test-Path $Path) {
        $InstalledSoftware += Get-ItemProperty $Path -ErrorAction SilentlyContinue | 
        Where-Object { $_.DisplayName -ne $null -and $_.SystemComponent -ne 1 } | 
        Select-Object DisplayName, DisplayVersion, Publisher
    }
}
$InstalledSoftware = $InstalledSoftware | Sort-Object DisplayName -Unique | Select-Object DisplayName, DisplayVersion

$SoftwareData = @{
    SoftwareCount     = $InstalledSoftware.Count
    InstalledSoftware = $InstalledSoftware
}
Send-InventoryIfChanged -PayloadType "Software" -Data $SoftwareData

# -------------------------------------------------------------------------
# 4. Drivers Inventory
# -------------------------------------------------------------------------
Write-Output "Gathering Drivers Inventory..."
$Drivers = Get-CimInstance Win32_PnPSignedDriver | Where-Object { $_.DeviceName -ne $null } | Select-Object DeviceName, Manufacturer, DriverVersion, DriverDate
$DriversData = @{
    DriverCount = $Drivers.Count
    Drivers     = $Drivers
}
Send-InventoryIfChanged -PayloadType "Drivers" -Data $DriversData

# -------------------------------------------------------------------------
# 5. Custom Inventory
# -------------------------------------------------------------------------
Write-Output "Gathering Custom Inventory..."
$LocalAdmins = Get-LocalGroupMember -Group "Administrators" -ErrorAction SilentlyContinue | Select-Object Name, PrincipalSource
$CustomData = @{
    LocalAdministrators = $LocalAdmins
}
Send-InventoryIfChanged -PayloadType "Custom" -Data $CustomData

# -------------------------------------------------------------------------
# 6. Audit Logging (ExecutionCompleted)
# -------------------------------------------------------------------------
Write-Output "Gathering Execution Audit Log..."
$ExecutionEndTime = Get-Date
$DurationSeconds = [math]::Round(($ExecutionEndTime - $ExecutionStartTime).TotalSeconds, 2)

$AuditData = @{
    Event                = "ExecutionCompleted"
    Status               = "Success"
    HardwareStatus       = $script:HardwareStatus
    SoftwareStatus       = $script:SoftwareStatus
    DriversStatus        = $script:DriversStatus
    CustomStatus         = $script:CustomStatus
    JitterDelaySeconds   = $JitterApplied
    TotalDurationSeconds = $DurationSeconds
}

$FinalAuditPayload = @{
    PayloadType = "Audit"
    EndpointId  = $EndpointId
    Data        = $AuditData
}

try {
    Invoke-RestMethod -Uri $ApiUrl -Method Post -Headers $Headers -Body ($FinalAuditPayload | ConvertTo-Json -Depth 5 -Compress) | Out-Null
    Write-Output "Execution sequence complete. Audit payload transmitted."
}
catch {
    Write-Error "Failed to transmit final Audit payload."
}
exit 0
