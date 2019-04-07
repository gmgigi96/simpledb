package simpledb.materialize;

import java.util.List;
import java.util.stream.Collectors;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class NWaysSortScan implements Scan {

	private List<Scan> s;
	private List<Boolean> hasNext;
	private Scan currentScan;
	private RecordComparator comp;

	public NWaysSortScan(List<TempTable> runs, RecordComparator comp) {
		this.comp = comp;
		s = runs.stream().map(run -> run.open()).collect(Collectors.toList());
		hasNext = s.stream().map(scan -> scan.next()).collect(Collectors.toList());
	}

	@Override
	public void beforeFirst() {
		s.forEach(scan -> scan.beforeFirst());
		hasNext = s.stream().map(scan -> scan.next()).collect(Collectors.toList());
	}

	@Override
	public boolean next() {
		if (currentScan != null) {
			int p = s.indexOf(currentScan);
			hasNext.set(p, currentScan.next());
		}

		int pos = posMin(s, hasNext);
		if (pos == -1)
			return false;
		currentScan = s.get(pos);

		return true;
	}

	private int posMin(List<Scan> srcs, List<Boolean> hasNext) {
		int pos_min = -1;
		Scan min = null;
		int i;

		for (i = 0; i < srcs.size(); i++) {
			if (hasNext.get(i)) {
				pos_min = i;
				min = srcs.get(i);
				break;
			}
		}

		for (; i < srcs.size(); i++) {
			if (hasNext.get(i)) {
				Scan tmp = srcs.get(i);
				if (comp.compare(tmp, min) < 0) {
					min = tmp;
					pos_min = i;
				}

			}
		}
		return pos_min;
	}

	@Override
	public void close() {
		s.forEach(scan -> scan.close());
	}

	@Override
	public Constant getVal(String fldname) {
		return currentScan.getVal(fldname);
	}

	@Override
	public int getInt(String fldname) {
		return currentScan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		return currentScan.getString(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return currentScan.hasField(fldname);
	}

}
