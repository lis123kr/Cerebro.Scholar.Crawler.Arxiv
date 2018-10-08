class Qtitle:
    Special_Char = None
    @classmethod
    def get_Special_Char(cls):
        if cls.Special_Char == None:
            import pickle as pk
            cls.Special_Char = pk.load(open("SpecialChar.pkl", "rb"))
        return cls.Special_Char