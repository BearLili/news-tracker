/** Additional rule observations only; never rewrite forecast/legacy targets. */
export function getRuleMetarTargets(targets) {
    const result = [...targets];
    if (!targets.some(target => target.station === 'KDEN')
        && targets.some(target => target.station === 'KBKF'
            && target.unit === 'F' && target.tz === 'America/Denver')) {
        // KDEN has its own reporting schedule. Do not inherit KBKF sliceMinute.
        result.push({ station: 'KDEN', unit: 'F', tz: 'America/Denver', name: 'Denver' });
    }
    return result;
}
