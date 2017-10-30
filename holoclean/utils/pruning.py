import math


class random_var:
	"""TODO:random_var class: class for random variable"""
	def __init__(self, **kwargs):
		self.__dict__.update(kwargs)

class Pruning:

    """TODO:Pruning class: Creates the domain table for all the cells"""
    def __init__(self,dataengine, spark_session,threshold=0.5):
	"""TODO.
	Parameters
	--------
	spark_session:Takes as an argument the spark_Session 
	threshold:The threshold that we will use for the pruning
	dataengine:Takes as an argument the Data Engine to create Domain table
	"""
	self.spark_session=spark_session
	self.dataengine=dataengine
        self.threshold = threshold
        self.assignments = {}
        self.cell_domain_nb = {}
        self.domain_stats = {}
        self.domain_pair_stats = {}
        self.col_to_cid = {}
        self.trgt_attr = {}
        self.trgt_cols = set([])
        self.cell_nbs = {}
        self.nb_cache = {}
        self.cell_domain = {}
        self.all_cells = []
	self.all_cells_temp={}
        
        print 'Analyzing associations of DB Entries...',
	self.noisycells=self._d_cell()
	self.final=[]
	self.cellvalues = self._c_values()
        self._preprop()
        self._analyzeEntries()
        self._generate_assignments()
        self._generate_nbs()
        self._find_cell_domain()
	domain=self._create_dataframe()
	dataengine.register_spark('Domain',domain)
        print 'DONE.'
 
    #Internal Method
    def _d_cell(self):
	"""
	Create noisy_cell list from the C_dk table
        """
	dataframe1=self.dataengine.get_table_spark("C_dk")
	dataframe1.show()
	noisy_cells=[]
	for c in dataframe1.collect():
		cell = random_var(columnname=c[1], row_id=int(c[0]))
		noisy_cells.append(cell)
	return noisy_cells

    def _c_values(self):
	"""
	Create c_value list from the init table
        """
	dataframe=self.dataengine.get_table_spark("Init")
	table_attribute_string=self.dataengine.get_schema('Init')
        table_attribute=table_attribute_string.split(',')
	rows=0
	cell_values={}
	number_id=0
	for c in dataframe.drop('index').collect():
		row={}
		j=1
		for i in c:			
			cell =random_var(columnname=table_attribute[j], value=i,tupleid=rows,cellid=number_id)
			row[j]=cell
			number_id=number_id+1
			j=j+1	
		cell_values[rows]=row
		rows=rows+1

	return cell_values
    def _compute_nb(self, a, v, a_trgt, v_trgt):
	"""TO DO: 
	generate_assignments creates assignment for each cell with the attribute and value
	of each other cell in the same row
	Parameters
	--------
	a: the name of first attribute
	v: the initial value of the first attribute
	a_trgt: the name of second attribute
	v_trgt: the initial value of the second attribute
        """
        if (v, v_trgt) not in self.domain_pair_stats[a][a_trgt]:
            return None
        cooccur_count = self.domain_pair_stats[a][a_trgt][(v, v_trgt)]
        v_cnt = self.domain_stats[a][v]
        v_trgt_cnt = self.domain_stats[a_trgt][v_trgt]

        # Compute nb
        p_ab = cooccur_count / len(self.cellvalues)
        p_a  = v_cnt / len(self.cellvalues)
        p_b = v_trgt_cnt / len(self.cellvalues)
        nb = math.log(p_ab/(p_a*p_b)) / -math.log(p_ab)
        return p_ab / p_a
        #return nb

    def _findDomain(self, assignment, trgt_attr):
	"""TO DO: _findDomain finds the domain for each cell
	Parameters
	--------
	assignment: attributes with value
	trgt_attr: the name of attribute
        """
        cell_values = set([assignment[trgt_attr]])
        for attr in assignment:
            if attr == trgt_attr:
                continue
            attr_val = assignment[attr]
            if attr in self.nb_cache:
                if attr_val in self.nb_cache[attr]:
                    if trgt_attr in self.nb_cache[attr][attr_val]:
                        cell_values |= set(self.nb_cache[attr][attr_val][trgt_attr].keys())
        return cell_values

    def _preprop(self):
	"""TO DO: 
	preprocessing phase. create the dictionary with all the attributes.
        """
        tpl = self.cellvalues[1]
        for cid in tpl:
            cell = tpl[cid]
            self.col_to_cid[cell.columnname] = cid
        for cell in self.noisycells:
            self.trgt_cols.add(cell.columnname)
        for col in self.col_to_cid:
            self.domain_stats[col] = {}
        for col1 in self.col_to_cid:
            self.domain_pair_stats[col1] = {}
            for col2 in self.trgt_cols:
                if col2 != col1:
                   self.domain_pair_stats[col1][col2] = {}  


    def _analyzeEntries(self):
        """TO DO: 
	analyzeEntries creates a dictionary with occurrences of the attributes
        """
	# Iterate over tuples
        for tupleid in self.cellvalues:
            # Iterate over attributes and grab counts
            for cid in self.cellvalues[tupleid]:
                cell = self.cellvalues[tupleid][cid]
                col = cell.columnname
                val = cell.value
                if col in self.trgt_cols: 
                    self.all_cells.append(cell)
		    self.all_cells_temp[cell.cellid]=cell
                if val not in self.domain_stats[col]:
                    self.domain_stats[col][val] = 0.0
                self.domain_stats[col][val] += 1.0
            # Iterate over target attributes and grab counts
            for col in self.domain_pair_stats:
                cid = self.col_to_cid[col]
                for tgt_col in self.domain_pair_stats[col]:
                    tgt_cid = self.col_to_cid[tgt_col]
                    tgt_val = self.cellvalues[tupleid][tgt_cid].value
                    val = self.cellvalues[tupleid][cid].value
                    assgn_tuple = (val, tgt_val)
                    if assgn_tuple not in self.domain_pair_stats[col][tgt_col]:
                        self.domain_pair_stats[col][tgt_col][assgn_tuple] = 0.0
                    self.domain_pair_stats[col][tgt_col][assgn_tuple] += 1.0


    def _generate_nbs(self):
	"""TO DO: 
	generate_nbs creates candidates repairs
        """
        for col in self.domain_pair_stats:
            self.nb_cache[col] = {}
            for tgt_col in self.domain_pair_stats[col]:
                for assgn_tuple in self.domain_pair_stats[col][tgt_col]:
                    nb = self._compute_nb(col, assgn_tuple[0], tgt_col, assgn_tuple[1])
                    if nb > self.threshold:
                        if assgn_tuple[0] not in self.nb_cache[col]:
                            self.nb_cache[col][assgn_tuple[0]] = {}
                        if tgt_col not in self.nb_cache[col][assgn_tuple[0]]:
                            self.nb_cache[col][assgn_tuple[0]][tgt_col] = {}
                        self.nb_cache[col][assgn_tuple[0]][tgt_col][assgn_tuple[1]] = nb
	
                    

    def _generate_assignments(self):
	"""
	generate_assignments creates assignment for each cell with the attribute and value
	of each other cell in the same row
        """
        for cell in self.all_cells:
            tplid = cell.tupleid
            trgt_attr = cell.columnname
            assignment = {}
            for cid in self.cellvalues[tplid]:
                c = self.cellvalues[tplid][cid]
                assignment[c.columnname] = c.value
            self.assignments[cell.cellid] = assignment
            self.trgt_attr[cell.cellid] = trgt_attr
	
 


    def _find_cell_domain(self):
	"""
	find_cell_domain finds the domain for each cell
        """
        for cellid in self.assignments:
            self.cell_domain[cellid] = self._findDomain(self.assignments[cellid], self.trgt_attr[cellid])

    def _create_dataframe(self):
	"""
	creates a spark dataframe from cell_domain
        """
	list_to_dataframe=[]
	for i in self.cell_domain:
		for j in self.cell_domain[i]:
				list_to_dataframe.append([(self.all_cells_temp[i].tupleid+1),self.all_cells_temp[i].columnname,j])
	new_df = self.spark_session.createDataFrame(list_to_dataframe,['tid','attr_name','attr_val'])
	new_df=new_df.orderBy("tid")
	new_df.show()	
	return new_df


