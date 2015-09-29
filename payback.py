import sqlite3
import sys
import time
import yaml

# XXX: To do - pass in by arg & argparse it
PVO_DB = sqlite3.connect(sys.argv[1])
PVO_DB.row_factory = sqlite3.Row
cursor = PVO_DB.cursor()

# XXX: To do - pass in by arg & argparse it
with open(sys.argv[2], "rb") as fh:
    TARIFF = yaml.safe_load(fh)

# XXX: To do - save to a database, and only gather results from where we need to (i.e. last calc)
cursor.execute('''
    SELECT * FROM pvoutput
        ORDER BY timestamp ASC
    ''')
try:
    rows = cursor.fetchall()
except Exception, e:
    print str(e)

index = 0
max = len(rows)
cum_save = 0
for n in range(0, max):
    r1 = rows[n]
    r2 = None
    paired = False
    if (r1[0] % 1800) == 0:
        for p in range(n, max):
            r2 = rows[p]
            if r2[0] < (r1[0] + 1800):
                continue
            elif r2[0] == (r1[0] + 1800):
                paired = True
                break
            else:
                break
    if paired:
        # v1 = Wh gen
        # v3 = Wh con
        # Wh net = (con - gen)
        gen = (float(r2[1]) - float(r1[1]))
        cons = (float(r2[3]) - float(r1[3]))
        net = cons - gen
        day = int(time.strftime("%w", time.localtime(r1[0])))
        hour = int(time.strftime("%H", time.localtime(r1[0])))
        
        # Find the right tariffs
        tariff = None
        for t in TARIFF:
            if (r1[0] >= t['start']) and (r1[0] < t['end']):
                tariff = t
                break
        if tariff is None:
            print "ERROR: no tarrif found for timestamp %d" % r1[0]
            exit(101)

        periods = None
        for t_label in t['times']:
            if day in t['times'][t_label]['days']:
                periods = t['times'][t_label]['periods']
                break
        if periods is None:
            print "ERROR: no periods found for day %d in label %s" % (
                day,
                t_label,
            )
            exit(102)

        rate = None
        for period in periods:
            if ((hour >= period['start']) and
                (hour < period['end'])):
                rate = t['rates'][period['rate']]
                break
        if rate is None:
            print "ERROR: no rate found for hour %d in day %d" % (
                hour,
                day,
            )
            exit(103)

        no_pv = (cons / 1000.0) * rate
        with_pv = (net / 1000.0) * rate
        if net < 0:
            with_pv = (net / 1000.0) * t['rates']['export']
        save = no_pv - with_pv
        cum_save += save

        print "%s - %s: cons=%.0f; gen=%.0f; net=%.0f; rate=%.2f; no_pv$=%.2f; with_pv$=%.2f; save$=%.2f; cum_save$=%.2f" % (
            time.strftime("%Y-%m-%d %H:%M", time.localtime(r1[0])),
            time.strftime("%Y-%m-%d %H:%M", time.localtime(r2[0])),
            cons,
            gen,
            net,
            (rate * 100),
            no_pv,
            with_pv,
            save,
            cum_save,
        )

cursor.close()
PVO_DB.close()

# XXX: To do - produce some reports: yearly, monthly, daily, etc.
