$Iterations = 10
$BaseArray = 1..1000 | ForEach-Object { [PSCustomObject]@{ DisplayName = "Software_$_"; DisplayVersion = "1.0" } }
$ArrayWithDupes = $BaseArray + $BaseArray + $BaseArray # 3000 items

Write-Host "Benchmarking O(N log N) Array Sort-Object -Unique..."
$OldTime = Measure-Command {
    for ($i = 0; $i -lt $Iterations; $i++) {
        $Result1 = $ArrayWithDupes | Sort-Object DisplayName -Unique | Select-Object DisplayName, DisplayVersion
    }
}
Write-Host "Old Method Avg Time: $([math]::Round($OldTime.TotalMilliseconds / $Iterations, 2)) ms"

Write-Host "`nBenchmarking O(N) .NET HashSet..."
$NewTime = Measure-Command {
    for ($i = 0; $i -lt $Iterations; $i++) {
        $SoftwareList = [System.Collections.Generic.List[PSCustomObject]]::new(3000)
        $SeenSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
        
        foreach ($Item in $ArrayWithDupes) {
            if ($SeenSet.Add($Item.DisplayName)) {
                $SoftwareList.Add([PSCustomObject]@{
                        DisplayName    = $Item.DisplayName
                        DisplayVersion = $Item.DisplayVersion
                    })
            }
        }
        $Result2 = $SoftwareList.ToArray()
    }
}
Write-Host "New Method Avg Time: $([math]::Round($NewTime.TotalMilliseconds / $Iterations, 2)) ms"

$Speedup = [math]::Round($OldTime.TotalMilliseconds / $NewTime.TotalMilliseconds, 2)
Write-Host "`nSpeedup Factor: ${Speedup}x faster"
