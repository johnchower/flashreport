let SessionLoad = 1
if &cp | set nocp | endif
let s:cpo_save=&cpo
set cpo&vim
inoremap <C-Space> 
imap <Nul> <C-Space>
inoremap <expr> <Up> pumvisible() ? "\" : "\<Up>"
inoremap <expr> <S-Tab> pumvisible() ? "\" : "\<S-Tab>"
inoremap <expr> <Down> pumvisible() ? "\" : "\<Down>"
nmap v <Plug>SlimeConfig
nmap  <Plug>SlimeParagraphSend
xmap  <Plug>SlimeRegionSend
nnoremap <silent>  :TmuxNavigateLeft
nnoremap <silent> <NL> :TmuxNavigateDown
nnoremap <silent>  :TmuxNavigateUp
nnoremap <silent>  :TmuxNavigateRight
nnoremap  :exec '!' .getline('.')
map  :NERDTreeToggle
nnoremap <silent>  :TmuxNavigatePrevious
nnoremap <silent> <c-\\> :TmuxNavigatePrevious
xmap S <Plug>VSurround
nnoremap \d :YcmShowDetailedDiagnostic
nmap cS <Plug>CSurround
nmap cs <Plug>Csurround
nmap ds <Plug>Dsurround
vmap gx <Plug>NetrwBrowseXVis
nmap gx <Plug>NetrwBrowseX
xmap gS <Plug>VgSurround
nmap ySS <Plug>YSsurround
nmap ySs <Plug>YSsurround
nmap yss <Plug>Yssurround
nmap yS <Plug>YSurround
nmap ys <Plug>Ysurround
vnoremap <silent> <Plug>NetrwBrowseXVis :call netrw#BrowseXVis()
nnoremap <silent> <Plug>NetrwBrowseX :call netrw#BrowseX(expand((exists("g:netrw_gx")? g:netrw_gx : '<cfile>')),netrw#CheckIfRemote())
nnoremap <silent> <Plug>SurroundRepeat .
vmap <BS> "-d
imap S <Plug>ISurround
imap s <Plug>Isurround
inoremap <expr> 	 pumvisible() ? "\" : "\	"
imap  <Plug>Isurround
let &cpo=s:cpo_save
unlet s:cpo_save
set autoindent
set background=dark
set backspace=indent,eol,start
set completefunc=youcompleteme#Complete
set completeopt=preview,menuone
set cpoptions=aAceFsB
set expandtab
set fileencodings=ucs-bom,utf-8,default,latin1
set helplang=en
set langmenu=none
set runtimepath=~/.vim,~/.vim/bundle/Vundle.vim,~/.vim/bundle/nerdtree,~/.vim/bundle/indentpython.vim,~/.vim/bundle/Nvim-R,~/.vim/bundle/vim-slime,~/.vim/bundle/vim-tmux-navigator,~/.vim/bundle/vim-surround,~/.vim/bundle/vim-colors-solarized,~/.vim/bundle/YouCompleteMe,/usr/local/Cellar/macvim/8.0-113/MacVim.app/Contents/Resources/vim/vimfiles,/usr/local/Cellar/macvim/8.0-113/MacVim.app/Contents/Resources/vim/runtime,/usr/local/Cellar/macvim/8.0-113/MacVim.app/Contents/Resources/vim/vimfiles/after,~/.vim/after,~/.vim/bundle/Vundle.vim,~/.vim/bundle/Vundle.vim/after,~/.vim/bundle/nerdtree/after,~/.vim/bundle/indentpython.vim/after,~/.vim/bundle/Nvim-R/after,~/.vim/bundle/vim-slime/after,~/.vim/bundle/vim-tmux-navigator/after,~/.vim/bundle/vim-surround/after,~/.vim/bundle/vim-colors-solarized/after,~/.vim/bundle/YouCompleteMe/after
set shiftwidth=2
set shortmess=filnxtToOc
set showmatch
set softtabstop=2
set splitbelow
set splitright
set textwidth=79
set updatetime=2000
set wildmenu
set window=53
let s:so_save = &so | let s:siso_save = &siso | set so=0 siso=0
let v:this_session=expand("<sfile>:p")
silent only
cd ~/Projects/flashreport
if expand('%') == '' && !&modified && line('$') <= 1 && getline(1) == ''
  let s:wipebuf = bufnr('%')
endif
set shortmess=aoO
badd +1 scratch_work/report_by_SQL_draft.r
badd +34 R/query_functions.r
badd +0 R_doc
badd +4 NAMESPACE
badd +12 DESCRIPTION
argglobal
silent! argdel *
set stal=2
edit R/query_functions.r
set splitbelow splitright
wincmd t
set winheight=1 winwidth=1
argglobal
vnoremap <buffer> <silent> \rd :call RSetWD()
vnoremap <buffer> <silent> \ko :call RMakeRmd("odt")
vnoremap <buffer> <silent> \kh :call RMakeRmd("html_document")
vnoremap <buffer> <silent> \kw :call RMakeRmd("word_document")
vnoremap <buffer> <silent> \kl :call RMakeRmd("beamer_presentation")
vnoremap <buffer> <silent> \kp :call RMakeRmd("pdf_document")
vnoremap <buffer> <silent> \kr :call RMakeRmd("default")
vnoremap <buffer> <silent> \r- :call RBrOpenCloseLs(0)
vnoremap <buffer> <silent> \r= :call RBrOpenCloseLs(1)
vnoremap <buffer> <silent> \ro :call RObjBrowser()
vnoremap <buffer> <silent> \rb :call RAction("plotsumm")
vnoremap <buffer> <silent> \rg :call RAction("plot")
vnoremap <buffer> <silent> \rs :call RAction("summary")
vnoremap <buffer> <silent> \rh :call RAction("help")
vnoremap <buffer> <silent> \re :call RAction("example")
vnoremap <buffer> <silent> \ra :call RAction("args")
vnoremap <buffer> <silent> \rv :call RAction("viewdf")
vnoremap <buffer> <silent> \rt :call RAction("str")
vnoremap <buffer> <silent> \rn :call RAction("nvim.names")
vnoremap <buffer> <silent> \rp :call RAction("print")
vnoremap <buffer> <silent> \rm :call RClearAll()
vnoremap <buffer> <silent> \rr :call RClearConsole()
vnoremap <buffer> <silent> \rl :call g:SendCmdToR("ls()")
vnoremap <buffer> <silent> \o :call RWarningMsg("This command does not work over a selection of lines.")
vnoremap <buffer> <silent> \fa :call SendFunctionToR("echo", "down")
vnoremap <buffer> <silent> \fd :call SendFunctionToR("silent", "down")
vnoremap <buffer> <silent> \fe :call SendFunctionToR("echo", "stay")
vnoremap <buffer> <silent> \ff :call SendFunctionToR("silent", "stay")
vnoremap <buffer> <silent> \; :call MovePosRCodeComment("selection")
vnoremap <buffer> <silent> \xu :call RSimpleCommentLine("selection", "u")
vnoremap <buffer> <silent> \xc :call RSimpleCommentLine("selection", "c")
vnoremap <buffer> <silent> \xx :call RComment("selection")
vnoremap <buffer> <silent> \rw :call RQuit('save')
vnoremap <buffer> <silent> \rq :call RQuit('nosave')
vnoremap <buffer> <silent> \rc :call StartR("custom")
vnoremap <buffer> <silent> \rf :call StartR("R")
nnoremap <buffer> <silent> \rd :call RSetWD()
onoremap <buffer> <silent> \rd :call RSetWD()
nnoremap <buffer> <silent> \ko :call RMakeRmd("odt")
onoremap <buffer> <silent> \ko :call RMakeRmd("odt")
nnoremap <buffer> <silent> \kh :call RMakeRmd("html_document")
onoremap <buffer> <silent> \kh :call RMakeRmd("html_document")
nnoremap <buffer> <silent> \kw :call RMakeRmd("word_document")
onoremap <buffer> <silent> \kw :call RMakeRmd("word_document")
nnoremap <buffer> <silent> \kl :call RMakeRmd("beamer_presentation")
onoremap <buffer> <silent> \kl :call RMakeRmd("beamer_presentation")
nnoremap <buffer> <silent> \kp :call RMakeRmd("pdf_document")
onoremap <buffer> <silent> \kp :call RMakeRmd("pdf_document")
nnoremap <buffer> <silent> \kr :call RMakeRmd("default")
onoremap <buffer> <silent> \kr :call RMakeRmd("default")
nnoremap <buffer> <silent> \r- :call RBrOpenCloseLs(0)
onoremap <buffer> <silent> \r- :call RBrOpenCloseLs(0)
nnoremap <buffer> <silent> \r= :call RBrOpenCloseLs(1)
onoremap <buffer> <silent> \r= :call RBrOpenCloseLs(1)
nnoremap <buffer> <silent> \ro :call RObjBrowser()
onoremap <buffer> <silent> \ro :call RObjBrowser()
nnoremap <buffer> <silent> \rb :call RAction("plotsumm")
onoremap <buffer> <silent> \rb :call RAction("plotsumm")
nnoremap <buffer> <silent> \rg :call RAction("plot")
onoremap <buffer> <silent> \rg :call RAction("plot")
nnoremap <buffer> <silent> \rs :call RAction("summary")
onoremap <buffer> <silent> \rs :call RAction("summary")
nnoremap <buffer> <silent> \rh :call RAction("help")
onoremap <buffer> <silent> \rh :call RAction("help")
nnoremap <buffer> <silent> \re :call RAction("example")
onoremap <buffer> <silent> \re :call RAction("example")
nnoremap <buffer> <silent> \ra :call RAction("args")
onoremap <buffer> <silent> \ra :call RAction("args")
nnoremap <buffer> <silent> \rv :call RAction("viewdf")
onoremap <buffer> <silent> \rv :call RAction("viewdf")
nnoremap <buffer> <silent> \rt :call RAction("str")
onoremap <buffer> <silent> \rt :call RAction("str")
nnoremap <buffer> <silent> \rn :call RAction("nvim.names")
onoremap <buffer> <silent> \rn :call RAction("nvim.names")
nnoremap <buffer> <silent> \rp :call RAction("print")
onoremap <buffer> <silent> \rp :call RAction("print")
nnoremap <buffer> <silent> \rm :call RClearAll()
onoremap <buffer> <silent> \rm :call RClearAll()
nnoremap <buffer> <silent> \rr :call RClearConsole()
onoremap <buffer> <silent> \rr :call RClearConsole()
nnoremap <buffer> <silent> \rl :call g:SendCmdToR("ls()")
onoremap <buffer> <silent> \rl :call g:SendCmdToR("ls()")
let s:cpo_save=&cpo
set cpo&vim
noremap <buffer> <silent> \r<Right> :call RSendPartOfLine("right", 0)
noremap <buffer> <silent> \r<Left> :call RSendPartOfLine("left", 0)
nnoremap <buffer> <silent> \o :call SendLineToRAndInsertOutput()0
onoremap <buffer> <silent> \o :call SendLineToRAndInsertOutput()0
noremap <buffer> <silent> \d :call SendLineToR("down")0
noremap <buffer> <silent> \l :call SendLineToR("stay")
noremap <buffer> <silent> \pa :call SendParagraphToR("echo", "down")
noremap <buffer> <silent> \pd :call SendParagraphToR("silent", "down")
noremap <buffer> <silent> \pe :call SendParagraphToR("echo", "stay")
noremap <buffer> <silent> \pp :call SendParagraphToR("silent", "stay")
vnoremap <buffer> <silent> \so :call SendSelectionToR("echo", "stay", "NewtabInsert")
vnoremap <buffer> <silent> \sa :call SendSelectionToR("echo", "down")
vnoremap <buffer> <silent> \sd :call SendSelectionToR("silent", "down")
vnoremap <buffer> <silent> \se :call SendSelectionToR("echo", "stay")
vnoremap <buffer> <silent> \ss :call SendSelectionToR("silent", "stay")
nnoremap <buffer> <silent> \fa :call SendFunctionToR("echo", "down")
onoremap <buffer> <silent> \fa :call SendFunctionToR("echo", "down")
nnoremap <buffer> <silent> \fd :call SendFunctionToR("silent", "down")
onoremap <buffer> <silent> \fd :call SendFunctionToR("silent", "down")
nnoremap <buffer> <silent> \fe :call SendFunctionToR("echo", "stay")
onoremap <buffer> <silent> \fe :call SendFunctionToR("echo", "stay")
nnoremap <buffer> <silent> \ff :call SendFunctionToR("silent", "stay")
onoremap <buffer> <silent> \ff :call SendFunctionToR("silent", "stay")
noremap <buffer> <silent> \ba :call SendMBlockToR("echo", "down")
noremap <buffer> <silent> \bd :call SendMBlockToR("silent", "down")
noremap <buffer> <silent> \be :call SendMBlockToR("echo", "stay")
noremap <buffer> <silent> \bb :call SendMBlockToR("silent", "stay")
noremap <buffer> <silent> \ks :call RSpin()
noremap <buffer> <silent> \ao :call ShowRout()
noremap <buffer> <silent> \ae :call SendFileToR("echo")
noremap <buffer> <silent> \aa :call SendFileToR("silent")
nnoremap <buffer> <silent> \; :call MovePosRCodeComment("normal")
onoremap <buffer> <silent> \; :call MovePosRCodeComment("normal")
nnoremap <buffer> <silent> \xu :call RSimpleCommentLine("normal", "u")
onoremap <buffer> <silent> \xu :call RSimpleCommentLine("normal", "u")
nnoremap <buffer> <silent> \xc :call RSimpleCommentLine("normal", "c")
onoremap <buffer> <silent> \xc :call RSimpleCommentLine("normal", "c")
nnoremap <buffer> <silent> \xx :call RComment("normal")
onoremap <buffer> <silent> \xx :call RComment("normal")
nnoremap <buffer> <silent> \rw :call RQuit('save')
onoremap <buffer> <silent> \rw :call RQuit('save')
nnoremap <buffer> <silent> \rq :call RQuit('nosave')
onoremap <buffer> <silent> \rq :call RQuit('nosave')
nnoremap <buffer> <silent> \rc :call StartR("custom")
onoremap <buffer> <silent> \rc :call StartR("custom")
nnoremap <buffer> <silent> \rf :call StartR("R")
onoremap <buffer> <silent> \rf :call StartR("R")
inoremap <buffer> <silent>  =RCompleteArgs()
inoremap <buffer> <silent> _ :call ReplaceUnderS()a
let &cpo=s:cpo_save
unlet s:cpo_save
setlocal keymap=
setlocal noarabic
setlocal autoindent
setlocal backupcopy=
setlocal balloonexpr=
setlocal nobinary
setlocal nobreakindent
setlocal breakindentopt=
setlocal bufhidden=
setlocal buflisted
setlocal buftype=
setlocal nocindent
setlocal cinkeys=0{,0},0),:,0#,!^F,o,O,e
setlocal cinoptions=
setlocal cinwords=if,else,while,do,for,switch
setlocal colorcolumn=
setlocal comments=:#',:###,:##,:#
setlocal commentstring=#\ %s
setlocal complete=.,w,b,u,t,i
setlocal concealcursor=
setlocal conceallevel=0
setlocal completefunc=youcompleteme#Complete
setlocal nocopyindent
setlocal cryptmethod=
setlocal nocursorbind
setlocal nocursorcolumn
setlocal nocursorline
setlocal define=
setlocal dictionary=
setlocal nodiff
setlocal equalprg=
setlocal errorformat=
setlocal expandtab
if &filetype != 'r'
setlocal filetype=r
endif
setlocal fixendofline
setlocal foldcolumn=0
setlocal foldenable
setlocal foldexpr=0
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldmarker={{{,}}}
setlocal foldmethod=manual
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldtext=foldtext()
setlocal formatexpr=
setlocal formatoptions=cq
setlocal formatlistpat=^\\s*\\d\\+[\\]:.)}\\t\ ]\\s*
setlocal grepprg=
setlocal iminsert=2
setlocal imsearch=2
setlocal include=
setlocal includeexpr=
setlocal indentexpr=GetRIndent()
setlocal indentkeys=0{,0},:,!^F,o,O,e
setlocal noinfercase
setlocal iskeyword=@,48-57,_,.
setlocal keywordprg=
setlocal nolinebreak
setlocal nolisp
setlocal lispwords=
setlocal nolist
setlocal nomacmeta
setlocal makeprg=
setlocal matchpairs=(:),{:},[:]
setlocal modeline
setlocal modifiable
setlocal nrformats=bin,octal,hex
set number
setlocal number
setlocal numberwidth=4
setlocal omnifunc=CompleteR
setlocal path=
setlocal nopreserveindent
setlocal nopreviewwindow
setlocal quoteescape=\\
setlocal noreadonly
setlocal norelativenumber
setlocal norightleft
setlocal rightleftcmd=search
setlocal noscrollbind
setlocal shiftwidth=2
setlocal noshortname
setlocal signcolumn=auto
setlocal nosmartindent
setlocal softtabstop=2
setlocal nospell
setlocal spellcapcheck=[.?!]\\_[\\])'\"\	\ ]\\+
setlocal spellfile=
setlocal spelllang=en
setlocal statusline=
setlocal suffixesadd=
setlocal swapfile
setlocal synmaxcol=3000
if &syntax != 'r'
setlocal syntax=r
endif
setlocal tabstop=8
setlocal tagcase=
setlocal tags=
setlocal textwidth=79
setlocal thesaurus=
setlocal noundofile
setlocal undolevels=-123456
setlocal nowinfixheight
setlocal nowinfixwidth
setlocal wrap
setlocal wrapmargin=0
silent! normal! zE
let s:l = 1 - ((0 * winheight(0) + 26) / 52)
if s:l < 1 | let s:l = 1 | endif
exe s:l
normal! zt
1
normal! 0
tabnew
set splitbelow splitright
wincmd t
set winheight=1 winwidth=1
argglobal
enew
file R_doc
vnoremap <buffer> <silent> \ko :call RMakeRmd("odt")
nnoremap <buffer> <silent> \ko :call RMakeRmd("odt")
onoremap <buffer> <silent> \ko :call RMakeRmd("odt")
vnoremap <buffer> <silent> \kh :call RMakeRmd("html_document")
nnoremap <buffer> <silent> \kh :call RMakeRmd("html_document")
onoremap <buffer> <silent> \kh :call RMakeRmd("html_document")
vnoremap <buffer> <silent> \kw :call RMakeRmd("word_document")
nnoremap <buffer> <silent> \kw :call RMakeRmd("word_document")
onoremap <buffer> <silent> \kw :call RMakeRmd("word_document")
vnoremap <buffer> <silent> \kl :call RMakeRmd("beamer_presentation")
nnoremap <buffer> <silent> \kl :call RMakeRmd("beamer_presentation")
onoremap <buffer> <silent> \kl :call RMakeRmd("beamer_presentation")
vnoremap <buffer> <silent> \kp :call RMakeRmd("pdf_document")
nnoremap <buffer> <silent> \kp :call RMakeRmd("pdf_document")
onoremap <buffer> <silent> \kp :call RMakeRmd("pdf_document")
vnoremap <buffer> <silent> \kr :call RMakeRmd("default")
nnoremap <buffer> <silent> \kr :call RMakeRmd("default")
onoremap <buffer> <silent> \kr :call RMakeRmd("default")
vnoremap <buffer> <silent> \r- :call RBrOpenCloseLs(0)
nnoremap <buffer> <silent> \r- :call RBrOpenCloseLs(0)
onoremap <buffer> <silent> \r- :call RBrOpenCloseLs(0)
vnoremap <buffer> <silent> \r= :call RBrOpenCloseLs(1)
nnoremap <buffer> <silent> \r= :call RBrOpenCloseLs(1)
onoremap <buffer> <silent> \r= :call RBrOpenCloseLs(1)
vnoremap <buffer> <silent> \ro :call RObjBrowser()
nnoremap <buffer> <silent> \ro :call RObjBrowser()
onoremap <buffer> <silent> \ro :call RObjBrowser()
vnoremap <buffer> <silent> \rb :call RAction("plotsumm")
nnoremap <buffer> <silent> \rb :call RAction("plotsumm")
onoremap <buffer> <silent> \rb :call RAction("plotsumm")
vnoremap <buffer> <silent> \rg :call RAction("plot")
nnoremap <buffer> <silent> \rg :call RAction("plot")
onoremap <buffer> <silent> \rg :call RAction("plot")
vnoremap <buffer> <silent> \rs :call RAction("summary")
nnoremap <buffer> <silent> \rs :call RAction("summary")
onoremap <buffer> <silent> \rs :call RAction("summary")
vnoremap <buffer> <silent> \rh :call RAction("help")
nnoremap <buffer> <silent> \rh :call RAction("help")
onoremap <buffer> <silent> \rh :call RAction("help")
vnoremap <buffer> <silent> \re :call RAction("example")
nnoremap <buffer> <silent> \re :call RAction("example")
onoremap <buffer> <silent> \re :call RAction("example")
vnoremap <buffer> <silent> \ra :call RAction("args")
nnoremap <buffer> <silent> \ra :call RAction("args")
onoremap <buffer> <silent> \ra :call RAction("args")
vnoremap <buffer> <silent> \rv :call RAction("viewdf")
nnoremap <buffer> <silent> \rv :call RAction("viewdf")
onoremap <buffer> <silent> \rv :call RAction("viewdf")
vnoremap <buffer> <silent> \rt :call RAction("str")
nnoremap <buffer> <silent> \rt :call RAction("str")
onoremap <buffer> <silent> \rt :call RAction("str")
vnoremap <buffer> <silent> \rn :call RAction("nvim.names")
nnoremap <buffer> <silent> \rn :call RAction("nvim.names")
onoremap <buffer> <silent> \rn :call RAction("nvim.names")
vnoremap <buffer> <silent> \rp :call RAction("print")
nnoremap <buffer> <silent> \rp :call RAction("print")
onoremap <buffer> <silent> \rp :call RAction("print")
vnoremap <buffer> <silent> \rm :call RClearAll()
nnoremap <buffer> <silent> \rm :call RClearAll()
onoremap <buffer> <silent> \rm :call RClearAll()
vnoremap <buffer> <silent> \rr :call RClearConsole()
nnoremap <buffer> <silent> \rr :call RClearConsole()
onoremap <buffer> <silent> \rr :call RClearConsole()
vnoremap <buffer> <silent> \rl :call g:SendCmdToR("ls()")
nnoremap <buffer> <silent> \rl :call g:SendCmdToR("ls()")
onoremap <buffer> <silent> \rl :call g:SendCmdToR("ls()")
let s:cpo_save=&cpo
set cpo&vim
noremap <buffer> <silent> \r<Right> :call RSendPartOfLine("right", 0)
noremap <buffer> <silent> \r<Left> :call RSendPartOfLine("left", 0)
vnoremap <buffer> <silent> \o :call RWarningMsg("This command does not work over a selection of lines.")
nnoremap <buffer> <silent> \o :call SendLineToRAndInsertOutput()0
onoremap <buffer> <silent> \o :call SendLineToRAndInsertOutput()0
noremap <buffer> <silent> \d :call SendLineToR("down")0
noremap <buffer> <silent> \l :call SendLineToR("stay")
noremap <buffer> <silent> \pa :call SendParagraphToR("echo", "down")
noremap <buffer> <silent> \pd :call SendParagraphToR("silent", "down")
noremap <buffer> <silent> \pe :call SendParagraphToR("echo", "stay")
noremap <buffer> <silent> \pp :call SendParagraphToR("silent", "stay")
vnoremap <buffer> <silent> \so :call SendSelectionToR("echo", "stay", "NewtabInsert")
vnoremap <buffer> <silent> \sa :call SendSelectionToR("echo", "down")
vnoremap <buffer> <silent> \sd :call SendSelectionToR("silent", "down")
vnoremap <buffer> <silent> \se :call SendSelectionToR("echo", "stay")
vnoremap <buffer> <silent> \ss :call SendSelectionToR("silent", "stay")
vnoremap <buffer> <silent> \fa :call SendFunctionToR("echo", "down")
nnoremap <buffer> <silent> \fa :call SendFunctionToR("echo", "down")
onoremap <buffer> <silent> \fa :call SendFunctionToR("echo", "down")
vnoremap <buffer> <silent> \fd :call SendFunctionToR("silent", "down")
nnoremap <buffer> <silent> \fd :call SendFunctionToR("silent", "down")
onoremap <buffer> <silent> \fd :call SendFunctionToR("silent", "down")
vnoremap <buffer> <silent> \fe :call SendFunctionToR("echo", "stay")
nnoremap <buffer> <silent> \fe :call SendFunctionToR("echo", "stay")
onoremap <buffer> <silent> \fe :call SendFunctionToR("echo", "stay")
vnoremap <buffer> <silent> \ff :call SendFunctionToR("silent", "stay")
nnoremap <buffer> <silent> \ff :call SendFunctionToR("silent", "stay")
onoremap <buffer> <silent> \ff :call SendFunctionToR("silent", "stay")
noremap <buffer> <silent> \ba :call SendMBlockToR("echo", "down")
noremap <buffer> <silent> \bd :call SendMBlockToR("silent", "down")
noremap <buffer> <silent> \be :call SendMBlockToR("echo", "stay")
noremap <buffer> <silent> \bb :call SendMBlockToR("silent", "stay")
nmap <buffer> <silent> ge :call RDocExSection()
nmap <buffer> <silent> q :q
let &cpo=s:cpo_save
unlet s:cpo_save
setlocal keymap=
setlocal noarabic
setlocal autoindent
setlocal backupcopy=
setlocal balloonexpr=
setlocal nobinary
setlocal nobreakindent
setlocal breakindentopt=
setlocal bufhidden=wipe
setlocal buflisted
setlocal buftype=nofile
setlocal nocindent
setlocal cinkeys=0{,0},0),:,0#,!^F,o,O,e
setlocal cinoptions=
setlocal cinwords=if,else,while,do,for,switch
setlocal colorcolumn=
setlocal comments=s1:/*,mb:*,ex:*/,://,b:#,:%,:XCOMM,n:>,fb:-
setlocal commentstring=/*%s*/
setlocal complete=.,w,b,u,t,i
setlocal concealcursor=nvc
setlocal conceallevel=2
setlocal completefunc=youcompleteme#Complete
setlocal nocopyindent
setlocal cryptmethod=
setlocal nocursorbind
setlocal nocursorcolumn
setlocal nocursorline
setlocal define=
setlocal dictionary=
setlocal nodiff
setlocal equalprg=
setlocal errorformat=
setlocal expandtab
if &filetype != 'rdoc'
setlocal filetype=rdoc
endif
setlocal fixendofline
setlocal foldcolumn=0
setlocal foldenable
setlocal foldexpr=0
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldmarker={{{,}}}
setlocal foldmethod=manual
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldtext=foldtext()
setlocal formatexpr=
setlocal formatoptions=tcq
setlocal formatlistpat=^\\s*\\d\\+[\\]:.)}\\t\ ]\\s*
setlocal grepprg=
setlocal iminsert=2
setlocal imsearch=2
setlocal include=
setlocal includeexpr=
setlocal indentexpr=
setlocal indentkeys=0{,0},:,0#,!^F,o,O,e
setlocal noinfercase
setlocal iskeyword=@,48-57,_,.
setlocal keywordprg=
setlocal nolinebreak
setlocal nolisp
setlocal lispwords=
setlocal nolist
setlocal nomacmeta
setlocal makeprg=
setlocal matchpairs=(:),{:},[:]
setlocal modeline
setlocal modifiable
setlocal nrformats=bin,octal,hex
set number
setlocal number
setlocal numberwidth=4
setlocal omnifunc=CompleteR
setlocal path=
setlocal nopreserveindent
setlocal nopreviewwindow
setlocal quoteescape=\\
setlocal noreadonly
setlocal norelativenumber
setlocal norightleft
setlocal rightleftcmd=search
setlocal noscrollbind
setlocal shiftwidth=2
setlocal noshortname
setlocal signcolumn=auto
setlocal nosmartindent
setlocal softtabstop=2
setlocal nospell
setlocal spellcapcheck=[.?!]\\_[\\])'\"\	\ ]\\+
setlocal spellfile=
setlocal spelllang=en
setlocal statusline=
setlocal suffixesadd=
setlocal noswapfile
setlocal synmaxcol=3000
if &syntax != 'rdoc'
setlocal syntax=rdoc
endif
setlocal tabstop=8
setlocal tagcase=
setlocal tags=
setlocal textwidth=79
setlocal thesaurus=
setlocal noundofile
setlocal undolevels=-123456
setlocal nowinfixheight
setlocal nowinfixwidth
setlocal wrap
setlocal wrapmargin=0
tabnext 1
set stal=1
if exists('s:wipebuf')
  silent exe 'bwipe ' . s:wipebuf
endif
unlet! s:wipebuf
set winheight=1 winwidth=20 shortmess=filnxtToOc
let s:sx = expand("<sfile>:p:r")."x.vim"
if file_readable(s:sx)
  exe "source " . fnameescape(s:sx)
endif
let &so = s:so_save | let &siso = s:siso_save
doautoall SessionLoadPost
unlet SessionLoad
" vim: set ft=vim :
