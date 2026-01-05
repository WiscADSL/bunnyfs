# disable hyperthreading
# echo off | sudo tee /sys/devices/system/cpu/smt/control
# for reading cpu performance counter
sudo modprobe msr
sudo sysctl kernel.nmi_watchdog=0

TARGET_FREQ="2900000"
for x in /sys/devices/system/cpu/*/cpufreq; do
	echo "$TARGET_FREQ" | sudo tee "$x/scaling_max_freq" > /dev/null 2>&1
done
