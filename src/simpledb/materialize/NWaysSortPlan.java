package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import simpledb.multibuffer.BufferNeeds;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class NWaysSortPlan implements Plan {

	private Plan p;
	private Transaction tx;
	private RecordComparator comp;
	private Schema sch;

	public NWaysSortPlan(Plan p, List<String> sortfields, Transaction tx) {
		this.p = p;
		this.tx = tx;
		this.comp = new RecordComparator(sortfields);
		this.sch = p.schema();
	}

	@Override
	public Scan open() {
		Scan src = p.open();
		int k = BufferNeeds.bestRoot(p.recordsOutput());
		int r = p.recordsOutput() / k;

		List<TempTable> runs = splitIntoRuns(src, r);

		src.close();

		while (runs.size() > k) {
			runs = doAMergeIteration(runs, k);
		}
		return new NWaysSortScan(runs, comp);
	}

	private List<TempTable> doAMergeIteration(List<TempTable> runs, int k) {
		List<TempTable> result = new ArrayList<TempTable>();

		while (runs.size() > k) {
			List<TempTable> p = popFirstKElem(runs, k);
			result.add(mergeKRuns(p));
		}

		if (runs.size() < k) {
			result.addAll(runs);
		}

		return result;
	}

	private TempTable mergeKRuns(List<TempTable> runs) {
		List<Scan> srcs = runs.stream().map(run -> run.open()).collect(Collectors.toList());
		TempTable result = new TempTable(sch, tx);
		UpdateScan dest = result.open();

		List<Boolean> hasNext = srcs.stream().map(src -> src.next()).collect(Collectors.toList());

		while (hasNext.stream().filter(s -> s).count() > 1) {
			// seleziono il minimo e lo aggiungo a result
			int pos = posMin(srcs, hasNext);
			Scan src = srcs.get(pos);
			hasNext.set(pos, copy(src, dest));
		}

		// una table è rimasta
		int pos = hasNext.indexOf(true);
		Scan src = srcs.get(pos);

		while (hasNext.get(pos)) {
			hasNext.set(pos, copy(src, dest));
		}

		srcs.stream().forEach(s -> s.close());
		dest.close();
		return result;
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

	private List<TempTable> popFirstKElem(List<TempTable> runs, int k) {
		List<TempTable> res = new ArrayList<>();
		for (int i = 0; i < k; i++)
			res.add(runs.remove(0));
		return res;
	}

	@Override
	public int blocksAccessed() {
		// does not include the one-time cost of sorting
		Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
		return mp.blocksAccessed();
	}

	@Override
	public int recordsOutput() {
		return p.recordsOutput();
	}

	@Override
	public int distinctValues(String fldname) {
		return p.distinctValues(fldname);
	}

	@Override
	public Schema schema() {
		return sch;
	}

	/**
	 * Splits a source scan src into many runs.
	 * 
	 * @param src  the source scan
	 * @param runs the number of runs
	 * @return the list of created runs
	 */
	protected List<TempTable> splitIntoRuns(Scan src, int runs) {
		List<TempTable> temps = new ArrayList<TempTable>();
		src.beforeFirst();
		boolean next = src.next();
		if (!next)
			return temps;

		/* k k-block runs */
		int k = runs;

		// p is the underlying plan
		int recordsPerRun = p.recordsOutput() / k;
		int copiedRecords = 0, createdRuns = 0;

		while (createdRuns < k) {

			/* create a target run */
			TempTable currenttemp = new TempTable(sch, tx);
			temps.add(currenttemp);
			UpdateScan currentscan = currenttemp.open();
			copiedRecords = 0;

			/* copy each run */
			while ((copiedRecords < recordsPerRun || createdRuns == k - 1) && next) {
				next = copy(src, currentscan);
				copiedRecords++;
			}

			/* sort the run */
			boolean anySwap = true;
			while (anySwap) {
				anySwap = false;
				currentscan.beforeFirst();
				RID curRid = null, nextRid = null;
				while (currentscan.next()) {
					curRid = currentscan.getRid();
					if (currentscan.next()) {
						nextRid = currentscan.getRid();
						if (comp.compare(currentscan, curRid, nextRid) > 0) {
							swap(currentscan, curRid, nextRid);
							anySwap = true;
						}
					}
					currentscan.moveToRid(curRid);
				}
			}
			currentscan.close();
			createdRuns++;
		}
		return temps;
	}

	/**
	 * Swaps the content of two records in UpdateScan given the two RIDS
	 * 
	 * @param s  the UpdateScan
	 * @param r1 the first RID
	 * @param r2 the second RID
	 */
	private void swap(UpdateScan s, RID r1, RID r2) {
		RID currentRid = s.getRid();

		s.moveToRid(r1);
		Map<String, Constant> values1 = new HashMap<String, Constant>();
		for (String fldName : p.schema().fields()) {
			values1.put(fldName, s.getVal(fldName));
		}

		s.moveToRid(r2);
		Map<String, Constant> values2 = new HashMap<String, Constant>();
		for (String fldName : p.schema().fields()) {
			values2.put(fldName, s.getVal(fldName));
			s.setVal(fldName, values1.get(fldName));
		}

		s.moveToRid(r1);
		for (String fldName : p.schema().fields()) {
			s.setVal(fldName, values2.get(fldName));
		}

		s.moveToRid(currentRid);
	}

	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : sch.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}

}
