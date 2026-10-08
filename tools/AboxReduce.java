import java.io.*;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.FunctionalSyntaxDocumentFormat;
import org.semanticweb.owlapi.model.*;

/**
 * Optional ABox-level optimisation for materialisation: individuals that are indistinguishable (same asserted types, same data shape and
 * the same neighbour classes, i.e. the same colour after colour refinement / bisimulation) are represented by one individual.
 * The reduced ontology is reasoned over (materialised) and the result is expanded to all members of each class afterwards.
 *
 *   reduce:  java AboxReduce reduce  in.owl reduced.ofn map.tsv
 *   expand:  java AboxReduce expand  in.owl map.tsv reduced_materialized.nt full_materialized.nt
 *
 * Individuals are never merged (pinned) when exact reasoning about them could differ between members: individuals occurring in TBox/RBox axioms
 * (nominals), in sameAs/differentFrom/negative assertions, with complex class assertions mentioning individuals, in classes with keys, or that are
 * incident to an edge of a "sensitive" role (cardinality restrictions, functional, inverse functional, asymmetric, irreflexive, reflexive, hasSelf,
 * disjoint, transitive and role chains, and all roles related to them by sub-property / inverse / equivalence).
 * Data values are only compared by shape (property, datatype, count up to 2) unless the data property is used inside a class expression
 * (then the exact values are compared).
 */
public class AboxReduce {
	static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	static final String OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs";

	public static void main(String[] a) throws Exception {
		if (a.length >= 4 && a[0].equals("reduce")) {
			boolean noop = false; Set<String> only = new HashSet<>();
			for (int i = 4; i < a.length; i++) { if (a[i].equals("noop")) noop = true; else if (a[i].startsWith("only=")) only.addAll(Arrays.asList(a[i].substring(5).split(","))); }
			reduce(a[1], a[2], a[3], noop, only);
		} else if (a.length >= 2 && a[0].equals("report")) report(a[1]);
		else if (a.length >= 5 && a[0].equals("expand")) expand(a[1], a[2], a[3], a[4]);
		else System.err.println("usage: report in.owl | reduce in out.ofn map.tsv [only=ClassA,ClassB] | expand in.owl map.tsv reduced.nt full.nt");
	}

	static OWLOntology load(String f) throws Exception {
		OWLOntologyManager m = OWLManager.createOWLOntologyManager();
		m.getOntologyConfigurator().setMissingImportHandlingStrategy(MissingImportHandlingStrategy.SILENT);
		return m.loadOntologyFromOntologyDocument(new File(f));
	}

	static String iri(OWLNamedObject o) { return o.getIRI().toString(); }

	/** analysis result shared by reduce and expand */
	static class Model {
		OWLOntology ont;
		Map<String, Set<String>> types = new HashMap<>();
		Map<String, List<String[]>> data = new HashMap<>();      // ind -> [prop, datatype/lang, lexical]
		List<String[]> edges = new ArrayList<>();                 // [s, role, o]
		Set<String> inds = new TreeSet<>();
		Set<String> pinned = new HashSet<>();
		Set<String> sensitive = new HashSet<>();
		Set<String> exactData = new HashSet<>();
		Map<String, Integer> color = new HashMap<>();
		Map<String, String> rep = new HashMap<>();
		Set<String> keyClasses = new HashSet<>();
		Map<String, Set<String>> subclassOf = new HashMap<>();   // super -> subs (named only)
		Map<String, String> pinReason = new HashMap<>();
		Map<String, Integer> sensitiveRoleHits = new TreeMap<>();
		Set<String> onlyClasses = new HashSet<>();
		Set<String> keyDataProps = new HashSet<>();
		boolean keyHasObjectProperties = false;
	}

	static void pin(Model M, String ind, String reason) { M.pinned.add(ind); M.pinReason.putIfAbsent(ind, reason); }

	static String roleName(OWLObjectPropertyExpression p) {
		OWLObjectPropertyExpression n = p.getNamedProperty();
		return n.asOWLObjectProperty().getIRI().toString();
	}

	static Model analyse(String file) throws Exception { return analyse(file, new HashSet<>()); }

	static Model analyse(String file, Set<String> onlyClasses) throws Exception {
		Model M = new Model();
		M.onlyClasses = onlyClasses;
		M.ont = load(file);
		Set<String> roleSeeds = new HashSet<>();
		Map<String, Set<String>> relation = new HashMap<>();
		List<OWLAxiom> tbox = new ArrayList<>();
		Set<String> keyClassNames = new HashSet<>();
		for (OWLAxiom ax : (Iterable<OWLAxiom>) M.ont.axioms()::iterator) {
			if (ax instanceof OWLClassAssertionAxiom) {
				OWLClassAssertionAxiom ca = (OWLClassAssertionAxiom) ax;
				if (!ca.getIndividual().isNamed()) { ca.individualsInSignature().forEach(i -> { if (i.isNamed()) pin(M, iri(i.asOWLNamedIndividual()), "class assertion on an anonymous individual"); }); continue; }
				String i = iri(ca.getIndividual().asOWLNamedIndividual());
				M.inds.add(i);
				if (ca.getClassExpression().isAnonymous()) {
					M.types.computeIfAbsent(i, k -> new TreeSet<>()).add("X:" + ca.getClassExpression().toString());
					ca.getClassExpression().individualsInSignature().forEach(j -> pin(M, iri(j), "used inside a complex class assertion"));
				} else {
					M.types.computeIfAbsent(i, k -> new TreeSet<>()).add(iri(ca.getClassExpression().asOWLClass()));
				}
			} else if (ax instanceof OWLObjectPropertyAssertionAxiom) {
				OWLObjectPropertyAssertionAxiom pa = (OWLObjectPropertyAssertionAxiom) ax;
				if (!pa.getSubject().isNamed() || !pa.getObject().isNamed()) {
					pa.individualsInSignature().forEach(j -> pin(M, iri(j), "role assertion with an anonymous individual")); continue;
				}
				String s = iri(pa.getSubject().asOWLNamedIndividual()), o = iri(pa.getObject().asOWLNamedIndividual());
				M.inds.add(s); M.inds.add(o);
				if (pa.getProperty().isAnonymous()) M.edges.add(new String[]{o, roleName(pa.getProperty()), s});
				else M.edges.add(new String[]{s, roleName(pa.getProperty()), o});
			} else if (ax instanceof OWLDataPropertyAssertionAxiom) {
				OWLDataPropertyAssertionAxiom da = (OWLDataPropertyAssertionAxiom) ax;
				if (!da.getSubject().isNamed()) continue;
				String s = iri(da.getSubject().asOWLNamedIndividual());
				M.inds.add(s);
				OWLLiteral l = da.getObject();
				String dt = l.hasLang() ? "@" + l.getLang() : l.getDatatype().getIRI().toString();
				M.data.computeIfAbsent(s, k -> new ArrayList<>()).add(new String[]{iri(da.getProperty().asOWLDataProperty()), dt, l.getLiteral()});
			} else if (ax instanceof OWLNegativeObjectPropertyAssertionAxiom || ax instanceof OWLNegativeDataPropertyAssertionAxiom
					|| ax instanceof OWLSameIndividualAxiom || ax instanceof OWLDifferentIndividualsAxiom) {
				ax.individualsInSignature().forEach(j -> { pin(M, iri(j), "sameAs / differentFrom / negative assertion"); M.inds.add(iri(j)); });
			} else if (ax instanceof OWLDeclarationAxiom || ax instanceof OWLAnnotationAssertionAxiom) {
				if (ax instanceof OWLDeclarationAxiom && ((OWLDeclarationAxiom) ax).getEntity().isOWLNamedIndividual()) M.inds.add(iri(((OWLDeclarationAxiom) ax).getEntity().asOWLNamedIndividual()));
			} else {
				tbox.add(ax);
			}
		}
		for (OWLAxiom ax : tbox) {
			ax.individualsInSignature().forEach(j -> { pin(M, iri(j), "mentioned in a TBox/RBox axiom (nominal)"); M.inds.add(iri(j)); });
			ax.nestedClassExpressions().forEach(ce -> {
				if (ce instanceof OWLObjectCardinalityRestriction) roleSeeds.add(roleName(((OWLObjectCardinalityRestriction) ce).getProperty()));
				else if (ce instanceof OWLObjectHasSelf) roleSeeds.add(roleName(((OWLObjectHasSelf) ce).getProperty()));
				else if (ce instanceof OWLDataRestriction || ce instanceof OWLDataCardinalityRestriction) {
					ce.dataPropertiesInSignature().forEach(p -> M.exactData.add(iri(p)));
				}
			});
			if (ax instanceof OWLFunctionalObjectPropertyAxiom || ax instanceof OWLInverseFunctionalObjectPropertyAxiom || ax instanceof OWLAsymmetricObjectPropertyAxiom
					|| ax instanceof OWLIrreflexiveObjectPropertyAxiom || ax instanceof OWLReflexiveObjectPropertyAxiom || ax instanceof OWLDisjointObjectPropertiesAxiom
					|| ax instanceof OWLTransitiveObjectPropertyAxiom || ax instanceof OWLSubPropertyChainOfAxiom) {
				ax.objectPropertiesInSignature().forEach(p -> roleSeeds.add(iri(p)));
			}
			if (ax instanceof OWLHasKeyAxiom) {
				OWLHasKeyAxiom hk = (OWLHasKeyAxiom) ax;
				hk.objectPropertiesInSignature().forEach(p -> { roleSeeds.add(iri(p)); M.keyHasObjectProperties = true; });
				hk.dataPropertiesInSignature().forEach(p -> M.keyDataProps.add(iri(p)));
				hk.getClassExpression().classesInSignature().forEach(c -> keyClassNames.add(iri(c)));
			}
			if (ax instanceof OWLFunctionalDataPropertyAxiom) ax.dataPropertiesInSignature().forEach(p -> M.exactData.add(iri(p)));
			if (ax instanceof OWLSubObjectPropertyOfAxiom) {
				OWLSubObjectPropertyOfAxiom so = (OWLSubObjectPropertyOfAxiom) ax;
				String s = roleName(so.getSubProperty()), p = roleName(so.getSuperProperty());
				relation.computeIfAbsent(s, k -> new HashSet<>()).add(p); relation.computeIfAbsent(p, k -> new HashSet<>()).add(s);
			} else if (ax instanceof OWLInverseObjectPropertiesAxiom || ax instanceof OWLEquivalentObjectPropertiesAxiom) {
				List<String> ps = ax.objectPropertiesInSignature().map(AboxReduce::iri).collect(Collectors.toList());
				for (String x : ps) for (String y : ps) if (!x.equals(y)) relation.computeIfAbsent(x, k -> new HashSet<>()).add(y);
			}
			if (ax instanceof OWLSubClassOfAxiom) {
				OWLSubClassOfAxiom sc = (OWLSubClassOfAxiom) ax;
				if (!sc.getSubClass().isAnonymous() && !sc.getSuperClass().isAnonymous())
					M.subclassOf.computeIfAbsent(iri(sc.getSuperClass().asOWLClass()), k -> new HashSet<>()).add(iri(sc.getSubClass().asOWLClass()));
			}
			if (ax instanceof OWLEquivalentClassesAxiom) {
				List<OWLClass> cs = ax.classesInSignature().collect(Collectors.toList());
				for (OWLClass c : cs) for (OWLClass d : cs) if (c != d) M.subclassOf.computeIfAbsent(iri(c), k -> new HashSet<>()).add(iri(d));
			}
		}
		// closure of sensitive roles over the role relation graph
		Deque<String> work = new ArrayDeque<>(roleSeeds);
		M.sensitive.addAll(roleSeeds);
		while (!work.isEmpty()) {
			String r = work.pop();
			for (String q : relation.getOrDefault(r, Collections.emptySet())) if (M.sensitive.add(q)) work.push(q);
		}
		// classes with keys (and their named subclasses): their instances are never merged
		Deque<String> cw = new ArrayDeque<>(keyClassNames);
		M.keyClasses.addAll(keyClassNames);
		while (!cw.isEmpty()) { String c = cw.pop(); for (String s : M.subclassOf.getOrDefault(c, Collections.emptySet())) if (M.keyClasses.add(s)) cw.push(s); }
		{
			// instances of classes with a key can only be merged by the key if two of them share all key values: pin those (and everybody if the key has object properties)
			Map<String, String> keyTuple = new HashMap<>();
			Map<String, Integer> keyTupleCount = new HashMap<>();
			for (Map.Entry<String, Set<String>> e : M.types.entrySet()) {
				boolean keyed = false;
				for (String t : e.getValue()) if (M.keyClasses.contains(t)) keyed = true;
				if (!keyed) continue;
				if (M.keyHasObjectProperties) { pin(M, e.getKey(), "instance of a class with a hasKey axiom over object properties"); continue; }
				List<String> vals = new ArrayList<>();
				for (String[] d : M.data.getOrDefault(e.getKey(), Collections.emptyList())) if (M.keyDataProps.contains(d[0])) vals.add(d[0] + "=" + d[2] + "^" + d[1]);
				Collections.sort(vals);
				String tuple = String.join(";", vals);
				keyTuple.put(e.getKey(), tuple); keyTupleCount.merge(tuple, 1, Integer::sum);
			}
			for (Map.Entry<String, String> e : keyTuple.entrySet()) if (e.getValue().isEmpty() || keyTupleCount.get(e.getValue()) > 1) pin(M, e.getKey(), "hasKey: key values missing or shared with another individual");
		}
		for (String[] e : M.edges) if (M.sensitive.contains(e[1])) { pin(M, e[0], "edge at a role with cardinality/functional/disjoint/transitive/chain axioms"); pin(M, e[2], "edge at a role with cardinality/functional/disjoint/transitive/chain axioms"); M.sensitiveRoleHits.merge(e[1], 1, Integer::sum); }
		if (!M.onlyClasses.isEmpty()) {
			for (String i : M.inds) {
				boolean wanted = false;
				for (String t : M.types.getOrDefault(i, Collections.emptySet())) for (String c : M.onlyClasses) if (t.equals(c) || t.endsWith("#" + c) || t.endsWith("/" + c)) wanted = true;
				if (!wanted) pin(M, i, "not an instance of the requested classes (only=...)");
			}
		}
		M.pinned.retainAll(M.inds);
		// colour refinement
		Map<String, Integer> intern = new HashMap<>();
		Map<String, List<String[]>> out = new HashMap<>(), in = new HashMap<>();
		for (String[] e : M.edges) { out.computeIfAbsent(e[0], k -> new ArrayList<>()).add(e); in.computeIfAbsent(e[2], k -> new ArrayList<>()).add(e); }
		for (String i : M.inds) {
			String sig;
			if (M.pinned.contains(i)) sig = "P:" + i;
			else {
				StringBuilder sb = new StringBuilder("T:").append(String.join("|", M.types.getOrDefault(i, Collections.emptySet())));
				Map<String, Integer> shape = new TreeMap<>();
				List<String> exact = new ArrayList<>();
				for (String[] d : M.data.getOrDefault(i, Collections.emptyList())) {
					if (M.exactData.contains(d[0])) exact.add(d[0] + "=" + d[2] + "^" + d[1]); else shape.merge(d[0] + "^" + d[1], 1, Integer::sum);
				}
				Collections.sort(exact);
				sb.append("#D:"); for (Map.Entry<String, Integer> s : shape.entrySet()) sb.append(s.getKey()).append("x").append(Math.min(2, s.getValue())).append(";");
				sb.append("#E:").append(String.join(";", exact));
				sig = sb.toString();
			}
			M.color.put(i, intern.computeIfAbsent(sig, k -> intern.size()));
		}
		int classes = new HashSet<>(M.color.values()).size();
		for (int round = 0; round < 100; round++) {
			Map<String, Integer> next = new HashMap<>(); Map<String, Integer> ids = new HashMap<>();
			for (String i : M.inds) {
				TreeSet<String> nb = new TreeSet<>();
				for (String[] e : out.getOrDefault(i, Collections.emptyList())) nb.add(e[1] + ">" + M.color.get(e[2]));
				for (String[] e : in.getOrDefault(i, Collections.emptyList())) nb.add(e[1] + "<" + M.color.get(e[0]));
				String sig = M.color.get(i) + "#" + String.join(",", nb);
				next.put(i, ids.computeIfAbsent(sig, k -> ids.size()));
			}
			M.color = next;
			int n2 = ids.size();
			if (n2 == classes) break;
			classes = n2;
		}
		// representatives: smallest IRI of each colour, pinned individuals represent themselves
		Map<Integer, String> first = new HashMap<>();
		for (String i : M.inds) {
			if (M.pinned.contains(i)) { M.rep.put(i, i); continue; }
			String r = first.computeIfAbsent(M.color.get(i), k -> i);
			M.rep.put(i, r);
		}
		return M;
	}

	static void reduce(String in, String outFile, String mapFile, boolean noop, Set<String> only) throws Exception {
		Model M = analyse(in, only);
		if (noop) for (String i : M.inds) M.rep.put(i, i);
		OWLOntologyManager m = M.ont.getOWLOntologyManager();
		OWLDataFactory df = m.getOWLDataFactory();
		Set<String> retained = new TreeSet<>();
		for (String i : M.inds) if (M.rep.get(i).equals(i)) retained.add(i);
		int removed = M.inds.size() - retained.size();
		// build the reduced ontology: all non-assertion axioms and the projected assertions of retained individuals
		OWLOntologyManager m2 = OWLManager.createOWLOntologyManager();
		OWLOntology red = m2.createOntology(M.ont.getOntologyID());
		List<OWLAxiom> keep = new ArrayList<>();
		for (OWLAxiom ax : (Iterable<OWLAxiom>) M.ont.axioms()::iterator) {
			if (ax instanceof OWLClassAssertionAxiom) {
				OWLClassAssertionAxiom ca = (OWLClassAssertionAxiom) ax;
				if (!ca.getIndividual().isNamed() || retained.contains(iri(ca.getIndividual().asOWLNamedIndividual()))) keep.add(ax);
			} else if (ax instanceof OWLDataPropertyAssertionAxiom) {
				OWLDataPropertyAssertionAxiom da = (OWLDataPropertyAssertionAxiom) ax;
				if (!da.getSubject().isNamed() || retained.contains(iri(da.getSubject().asOWLNamedIndividual()))) keep.add(ax);
			} else if (ax instanceof OWLObjectPropertyAssertionAxiom) {
				OWLObjectPropertyAssertionAxiom pa = (OWLObjectPropertyAssertionAxiom) ax;
				if (!pa.getSubject().isNamed() || !pa.getObject().isNamed()) { keep.add(ax); continue; }
				String s = iri(pa.getSubject().asOWLNamedIndividual()), o = iri(pa.getObject().asOWLNamedIndividual());
				String rs = M.rep.get(s), ro = M.rep.get(o);
				if (!s.equals(rs) && !retained.contains(s)) continue;      // source not retained: represented by its class representative
				if (o.equals(ro)) { keep.add(ax); continue; }              // target retained
				keep.add(df.getOWLObjectPropertyAssertionAxiom(pa.getProperty(), pa.getSubject(), df.getOWLNamedIndividual(IRI.create(ro))));
			} else keep.add(ax);
		}
		m2.addAxioms(red, keep.stream());
		m2.saveOntology(red, new FunctionalSyntaxDocumentFormat(), IRI.create(new File(outFile).toURI()));
		try (PrintWriter w = new PrintWriter(new OutputStreamWriter(new FileOutputStream(mapFile), StandardCharsets.UTF_8))) {
			for (String i : M.pinned) { w.print("PIN\t" + i + "\n"); }
			for (String i : M.inds) { w.print("REP\t" + i + "\t" + M.rep.get(i) + "\n"); }
			Set<String> genuine = new HashSet<>();
			for (String[] e : M.edges) { genuine.add(e[0] + "|" + e[2]); genuine.add(e[2] + "|" + e[0]); }
			Set<String> fake = new TreeSet<>();
			for (String[] e : M.edges) if (retained.contains(e[0]) && !retained.contains(e[2])) { String rv = M.rep.get(e[2]); if (!genuine.contains(e[0] + "|" + rv)) fake.add(e[0] + "\t" + rv); }
			for (String f : fake) w.print("FAKE\t" + f + "\n");
			for (String[] e : M.edges) if (!retained.contains(e[0]) || !retained.contains(e[2])) { w.print("EDGE\t" + e[0] + "\t" + e[1] + "\t" + e[2] + "\n"); }
		}
		long pinned = M.pinned.size();
		System.out.println("individuals " + M.inds.size() + ", pinned " + pinned + ", classes " + new HashSet<>(M.color.values()).size() + ", retained " + retained.size() + ", removed " + removed
				+ " (" + String.format("%.1f", 100.0 * removed / Math.max(1, M.inds.size())) + " %), sensitive roles " + M.sensitive.size());
	}


	/** prints how much an ABox could be reduced and why individuals cannot be merged */
	static void report(String in) throws Exception {
		Model M = analyse(in);
		Map<Integer, List<String>> byColour = new HashMap<>();
		for (String i : M.inds) if (!M.pinned.contains(i)) byColour.computeIfAbsent(M.color.get(i), k -> new ArrayList<>()).add(i);
		int merged = 0, mergeable = 0;
		for (List<String> g : byColour.values()) { if (g.size() > 1) { merged += g.size() - 1; mergeable += g.size(); } }
		int n = M.inds.size();
		System.out.println("Individuals: " + n + ", pinned (never merged): " + M.pinned.size() + ", free: " + (n - M.pinned.size()));
		System.out.println("Classes of indistinguishable free individuals: " + byColour.size() + "; individuals that can be removed: " + merged + " (" + String.format("%.1f", 100.0 * merged / Math.max(1, n)) + " % of all individuals)");
		List<Map.Entry<Integer, List<String>>> big = new ArrayList<>(byColour.entrySet());
		big.sort((x, y) -> y.getValue().size() - x.getValue().size());
		System.out.println("Largest classes (size, asserted types):");
		for (int k = 0; k < Math.min(8, big.size()); k++) {
			List<String> g = big.get(k).getValue();
			String ty = String.join(", ", M.types.getOrDefault(g.get(0), Collections.emptySet()).stream().map(t -> t.contains("#") ? t.substring(t.lastIndexOf('#') + 1) : t).collect(Collectors.toList()));
			System.out.println("  " + g.size() + "  " + (ty.isEmpty() ? "(no asserted type)" : ty));
		}
		Map<String, Integer> reasons = new TreeMap<>();
		for (String i : M.pinned) reasons.merge(M.pinReason.getOrDefault(i, "other"), 1, Integer::sum);
		System.out.println("Why individuals are never merged:");
		reasons.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).forEach(e -> System.out.println("  " + e.getValue() + "  " + e.getKey()));
		if (!M.sensitiveRoleHits.isEmpty()) {
			System.out.println("Roles whose edges block merging (edges):");
			M.sensitiveRoleHits.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(8).forEach(e -> System.out.println("  " + e.getValue() + "  " + e.getKey()));
		}
		String advice = merged * 5 >= n ? "worth trying: tools/materialize-reduced.sh" : "little to gain (under 20 % removable): materialize the full ABox";
		System.out.println("Advice: " + advice);
	}

	static final Pattern NT = Pattern.compile("^(<[^>]*>)\\s+(<[^>]*>)\\s+(.*?)\\s*\\.\\s*$");
	static String strip(String t) { return t.startsWith("<") && t.endsWith(">") ? t.substring(1, t.length() - 1) : t; }
	static String esc(String s) { return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t"); }

	static void expand(String in, String mapFile, String redNt, String fullNt) throws Exception {
		Model M = analyse(in);
		Map<String, String> rep = new HashMap<>();
		List<String[]> edges = new ArrayList<>();
		Set<String> fakePairs = new HashSet<>();
		Set<String> pinnedSet = new HashSet<>();
		for (String line : Files.readAllLines(new File(mapFile).toPath(), StandardCharsets.UTF_8)) {
			String[] p = line.split("\t");
			if (p[0].equals("REP")) rep.put(p[1], p[2]); else if (p[0].equals("EDGE")) edges.add(new String[]{p[1], p[2], p[3]}); else if (p[0].equals("PIN")) pinnedSet.add(p[1]); else if (p[0].equals("FAKE")) { fakePairs.add(p[1] + "|" + p[2]); fakePairs.add(p[2] + "|" + p[1]); }
		}
		Map<String, List<String>> members = new HashMap<>();     // representative -> removed members
		for (Map.Entry<String, String> e : rep.entrySet()) if (!e.getKey().equals(e.getValue())) members.computeIfAbsent(e.getValue(), k -> new ArrayList<>()).add(e.getKey());
		Set<String> removed = new HashSet<>(); members.values().forEach(removed::addAll);
		Map<String, List<String[]>> bySubject = new HashMap<>();   // subject iri -> [p, objectRaw]
		Map<String, Set<String>> pairRoles = new HashMap<>();      // s|o -> inferred roles between retained individuals
		List<String> lines = new ArrayList<>();
		List<String[]> spoList = new ArrayList<>();
		try (BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(redNt), StandardCharsets.UTF_8))) {
			String l;
			while ((l = r.readLine()) != null) {
				lines.add(l);
				Matcher mt = NT.matcher(l); if (!mt.matches()) continue;
				String s = strip(mt.group(1)), p = strip(mt.group(2)), o = mt.group(3);
				bySubject.computeIfAbsent(s, k -> new ArrayList<>()).add(new String[]{p, o});
				spoList.add(new String[]{s, p, o});
				if (o.startsWith("<")) pairRoles.computeIfAbsent(s + "|" + strip(o), k -> new HashSet<>()).add(p);
			}
		}
		try (PrintWriter w = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fullNt), StandardCharsets.UTF_8))) {
			for (String l : lines) {
				Matcher mt = NT.matcher(l);
				if (mt.matches() && mt.group(3).startsWith("<") && fakePairs.contains(strip(mt.group(1)) + "|" + strip(mt.group(3)))) continue;
				w.print(l); w.print('\n');
			}
			Set<String> emitted = new HashSet<>();
			java.util.function.BiConsumer<String, String[]> emit = (s, po) -> { String t = "<" + s + "> <" + po[0] + "> " + po[1] + " ."; if (emitted.add(t)) { w.print(t); w.print('\n'); } };
			// 1) copy types / sameAs of the representative to every removed member, and the asserted data of the member
			for (Map.Entry<String, List<String>> e : members.entrySet()) {
				for (String m : e.getValue()) {
					for (String[] po : bySubject.getOrDefault(e.getKey(), Collections.emptyList())) {
						if (po[0].equals(RDF_TYPE) || po[0].equals(OWL_SAMEAS)) emit.accept(m, po);
					}
					for (String[] d : M.data.getOrDefault(m, Collections.emptyList())) {
						String lit;
						String XS = "http://www.w3.org/2001/XMLSchema#string", PL = "http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral";
						if (d[1].startsWith("@")) lit = "\"" + esc(d[2]) + d[1] + "\"^^<" + PL + ">";
						else if (d[1].equals(XS) || d[1].equals(PL)) lit = "\"" + esc(d[2]) + "@\"^^<" + PL + ">";
						else lit = "\"" + esc(d[2]) + "\"^^<" + d[1] + ">";
						emit.accept(m, new String[]{d[0], lit});
					}
				}
			}
			// 1b) inferred role facts between a class representative and a pinned individual hold for every member of the class (e.g. hasValue restrictions)
			for (String[] spo : spoList) {
				String subj = spo[0], pred = spo[1], obj = spo[2];
				if (pred.equals(RDF_TYPE) || pred.equals(OWL_SAMEAS) || !obj.startsWith("<")) continue;
				String objIri = strip(obj);
				if (pinnedSet.contains(objIri) && members.containsKey(subj)) for (String m : members.get(subj)) emit.accept(m, new String[]{pred, obj});
				if (pinnedSet.contains(subj) && members.containsKey(objIri)) for (String m : members.get(objIri)) emit.accept(subj, new String[]{pred, "<" + m + ">"});
			}
			// 2) role assertions for every original asserted edge that involves a removed individual (asserted role plus the inferred roles of the representative pair)
			for (String[] e : edges) {
				String u = e[0], role = e[1], v = e[2];
				emit.accept(u, new String[]{role, "<" + v + ">"});
				String ru = rep.getOrDefault(u, u), rv = rep.getOrDefault(v, v);
				for (String q : pairRoles.getOrDefault(ru + "|" + rv, Collections.emptySet())) emit.accept(u, new String[]{q, "<" + v + ">"});
				for (String q : pairRoles.getOrDefault(rv + "|" + ru, Collections.emptySet())) emit.accept(v, new String[]{q, "<" + u + ">"});
			}
		}
		System.out.println("expanded " + removed.size() + " removed individuals");
	}
}
