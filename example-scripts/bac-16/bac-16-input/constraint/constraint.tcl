mol load pdb /home/dnanexus/output/default/egfr/com/drug/l01/tyk2-l01/build/complex.pdb
set a [atomselect top all]

set outfile [open tmp_cbv w]
set minmax [measure minmax $a]
set boxsize [vecsub [lindex $minmax 1] [lindex $minmax 0]]
set centre [measure center $a]

puts $outfile $boxsize
puts $outfile $centre

close $outfile

$a set beta 0
$a set occupancy 0
set b [atomselect top "(resid 1 to 288) and noh"]
$b set beta 4
set z [atomselect top "(not (resid 1 to 288) and not water and not type IM) and noh"]
$z set beta 4


$a writepdb /home/dnanexus/output/default/egfr/com/drug/l01/tyk2-l01/constraint/f4.pdb

quit
