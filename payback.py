import sqlite3
import time

PVO_DB = '/opt/energy/pvoutput.sqlite'
pvo_db = sqlite3.connect(PVO_DB)
pvo_db.row_factory = sqlite3.Row
cursor = pvo_db.cursor()

TARIFF = [
    (
        0,
        1420030800,
        {
            'peak': 0.3036,
            'offpeak': 0.1386,
            'peak_days': [1, 2, 3, 4, 5],
            'peak_times': [(7, 23)],
            'export': 0.08,
        },
    ),
    (
        1420030800,
        1735650000,
        {
            'peak': 0.308,
            'offpeak': 0.13915,
            'peak_days': [1, 2, 3, 4, 5],
            'peak_times': [(7, 23)],
            'export': 0.08,
        },
    ),
]

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
        for t in TARIFF:
            if (r1[0] >= t[0]) and (r1[0] < t[1]):
                tariff = t[2]
                break

        rate = tariff['offpeak']
        if day in tariff['peak_days']:
            for period in tariff['peak_times']:
                if ((hour >= period[0]) and
                    (hour < period[1])):
                    rate = tariff['peak']
                    break
        no_pv = (cons / 1000.0) * rate
        with_pv = (net / 1000.0) * rate
        if net < 0:
            with_pv = (net / 1000.0) * tariff['export']
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
pvo_db.close()
