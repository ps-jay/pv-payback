import argparse
import sqlite3
import sys
import time
import yaml

PARSER = argparse.ArgumentParser(description='Compute PV payback info')
PARSER.add_argument('database',
                    help='Path to pvoutput SQLite database',
                   )
PARSER.add_argument('tariff_file',
                    help='Path to YAML tariff file',
                   )
PARSER.add_argument('-s', '--start',
                    help='Start time in format: %%Y-%%m-%%d %%H:%%M %%Z (default: %(default)s)',
                    default='2014-04-04 04:04 AEDT',
                   )
PARSER.add_argument('-e', '--end',
                    help='End time in format: %%Y-%%m-%%d %%H:%%M %%Z (default: %(default)s)',
                    default='2022-02-02 02:02 AEDT',
                   )
ARGS = PARSER.parse_args()

PVO_DB = sqlite3.connect(ARGS.database)
PVO_DB.row_factory = sqlite3.Row
cursor = PVO_DB.cursor()

with open(ARGS.tariff_file, "rb") as fh:
    TARIFF = yaml.safe_load(fh)

START = time.mktime(time.strptime(ARGS.start, "%Y-%m-%d %H:%M %Z"))
END = time.mktime(time.strptime(ARGS.end, "%Y-%m-%d %H:%M %Z"))

# XXX: To do - save to a database, and only gather results from where we need to (i.e. last calc)
# XXX: To do - select based on start & end timestamps
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
cum_save = 0.0
cum_spend = 0.0
cum_exp = 0.0
cum_exp_earn = 0.0
cum_gen_used = 0.0
cum_gen_avoid = 0.0
cum_missed_block = 0.0
cum_gen = 0.0
cum_cons = 0.0
cum_imp = 0.0
cum_dsc = 0.0
curr_day = None
for n in range(0, max):
    r1 = rows[n]
    r2 = None
    paired = False
    if r1[0] < START:
        continue
    if r1[0] > END:
        break
    if (r1[0] % 1800) == 0:
        for p in range(n, max):
            r2 = rows[p]
            if r2[0] < (r1[0] + 1800):
                continue
            elif r2[0] == (r1[0] + 1800):
                paired = True
                break
            else:
                cum_missed_block += 1
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
            s = time.mktime(time.strptime(t['start'], "%Y-%m-%d %H:%M %Z"))
            e = time.mktime(time.strptime(t['end'], "%Y-%m-%d %H:%M %Z"))
            if (r1[0] >= s) and (r1[0] < e):
                tariff = t
                break
        if tariff is None:
            print "ERROR: no tarrif found for timestamp %d" % r1[0]
            exit(101)

        if day != curr_day:
            curr_day = day
            cum_dsc += tariff['rates']['dsc']

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
            cum_exp += net
            cum_exp_earn += with_pv
        else:
            cum_imp += net
        if gen > 0:
            used = gen
            if net < 0:
                used += net
            cum_gen_used += used
            cum_gen_avoid += (used / 1000.0) * rate
            cum_gen += gen

        cum_spend += with_pv
        save = no_pv - with_pv
        cum_save += save
        cum_cons += cons

        print "%s - %s: cons=%.0f;%sgen=%.0f;%snet=%.0f;\trate=%.2f;\tno_pv$=%.2f;\twith_pv$=%.2f;\tsave$=%.2f;\tcum_save$=%.2f\tcum_spend$=%.2f\tcum_exp=%.0f\tcum_avoid=%.0f\tcum_cons=%.0f\tcum_imp=%.0f\tcum_gen=%.0f" % (
            time.strftime("%Y-%m-%d %H:%M", time.localtime(r1[0])),
            time.strftime("%Y-%m-%d %H:%M", time.localtime(r2[0])),
            cons,
            ' ' * (6 - len("%.0f" % cons)),
            gen,
            ' ' * (5 - len("%.0f" % gen)),
            net,
            (rate * 100),
            no_pv,
            with_pv,
            save,
            cum_save,
            cum_spend,
            cum_exp * -1,
            cum_gen_used,
            cum_cons,
            cum_imp,
            cum_gen,
        )

print "Total missed blocks   : %d"    % cum_missed_block
print ""
print "Total earned by export: $%.2f" % (cum_exp_earn * -1)
print "Total avoided by use  : $%.2f" % cum_gen_avoid
print "Total saved (av+ex)   : $%.2f" % cum_save
print ""
print "Total import spend    : $%.2f" % cum_spend
print "Total daily svc chrg  : $%.2f" % cum_dsc
print ""
print "Total out-of-pocket   : $%.2f" % (cum_spend + cum_dsc)

cursor.close()
PVO_DB.close()

# XXX: To do - produce some reports: yearly, monthly, daily, etc.
